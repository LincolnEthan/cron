package cron

import (
	"log"
	"runtime"
	"sync"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries  Entries
	stop     chan struct{}
	add      chan *Entry
	reset    chan *Entry
	remove   chan string
	snapshot chan []*Entry
	running  bool
	ErrorLog *log.Logger
	location *time.Location

	mtx *sync.Mutex
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// New returns a new Cron job runner, in the Local time zone.
func New() *Cron {
	return NewWithLocation(time.Now().Location())
}

// NewWithLocation returns a new Cron job runner.
func NewWithLocation(location *time.Location) *Cron {
	return &Cron{
		entries:  make(Entries),
		add:      make(chan *Entry),
		reset:    make(chan *Entry),
		remove:   make(chan string),
		stop:     make(chan struct{}),
		snapshot: make(chan []*Entry),
		running:  false,
		ErrorLog: nil,
		location: location,
		mtx:      &sync.Mutex{},
	}
}

// A wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec string, cmd func()) (string, error) {
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec string, cmd Job) (string, error) {
	schedule, err := Parse(spec)
	if err != nil {
		return "", err
	}
	id := c.Schedule(schedule, cmd)
	return id, nil
}

func (c *Cron) Remove(id string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if !c.running {
		c.entries.Remove(id)
		return
	}

	c.remove <- id
}

func (c *Cron) ResetJob(spec string, id string) error {
	schedule, err := Parse(spec)
	if err != nil {
		return err
	}
	c.Reschedule(schedule, id)
	return nil
}

//Reschedule 重新安排时间
func (c *Cron) Reschedule(schedule Schedule, id string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if !c.running {
		if ent, ok := c.entries.Get(id); ok {
			ent.Schedule = schedule
		}
	}
	c.reset <- &Entry{
		Schedule: schedule,
		id:       id,
	}
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, cmd Job) string {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	entry := newEntry(schedule, cmd)
	if !c.running {
		c.entries.Add(entry)
		return entry.id
	}

	c.add <- entry
	return entry.id
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.running {
		c.snapshot <- nil
		x := <-c.snapshot
		return x
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Start the cron scheduler in its own go-routine, or no-op if already started.
func (c *Cron) Start() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.mtx.Lock()
	if c.running {
		c.mtx.Unlock()
		return
	}
	c.running = true
	c.mtx.Unlock()
	c.run()
}

func (c *Cron) runWithRecovery(j Job) {
	defer func() {
		if r := recover(); r != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			c.logf("cron: panic running job: %v\n%s", r, buf)
		}
	}()
	j.Run()
}

// Run the scheduler. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
	}

	for {
		// Determine the next entry to run.
		entries := c.entries.Sort()

		var timer *time.Timer
		if len(entries) == 0 || entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			timer = time.NewTimer(entries[0].Next.Sub(now))
		}

		for {
			select {
			case now = <-timer.C:
				now = now.In(c.location)
				// Run every entry whose next time was less than now
				for _, e := range entries {
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					go c.runWithRecovery(e.Job)
					e.Prev = e.Next
					e.Next = e.Schedule.Next(now)
				}
			case entryID := <-c.remove:
				c.entries.Remove(entryID)

			case entry := <-c.reset:
				if ent, ok := c.entries.Get(entry.id); ok {
					timer.Stop()
					now = c.now()
					ent.Schedule = entry.Schedule // 重置schedule
					ent.Next = ent.Schedule.Next(now)
				}
			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry.Next = newEntry.Schedule.Next(now)
				c.entries.Add(newEntry)

			case <-c.snapshot:
				c.snapshot <- c.entrySnapshot()
				continue

			case <-c.stop:
				timer.Stop()
				return
			}

			break
		}
	}
}

// Logs an error to stderr or to the configured error log
func (c *Cron) logf(format string, args ...interface{}) {
	if c.ErrorLog != nil {
		c.ErrorLog.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
func (c *Cron) Stop() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if !c.running {
		return
	}
	c.stop <- struct{}{}
	c.running = false
}

//GracefulStop 优雅的stop
func (c *Cron) GracefulStop() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if !c.running {
		return
	}

	c.stop <- struct{}{}
	c.running = false
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []*Entry {
	entries := []*Entry{}
	for _, e := range c.entries.Sort() {
		entries = append(entries, &Entry{
			Schedule: e.Schedule,
			Next:     e.Next,
			Prev:     e.Prev,
			Job:      e.Job,
		})
	}
	return entries
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}
