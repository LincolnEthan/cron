package cron

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)

var timeBase = time.Date(1582, time.October, 15, 0, 0, 0, 0, time.UTC).Unix()
var hardwareAddr []byte
var clockSeq uint32

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// The schedule on which this job should be run.
	Schedule Schedule

	// The next time the job will run. This is the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// The last time this job was run. This is the zero time if the job has never
	// been run.
	Prev time.Time

	// The Job to run.
	Job Job

	id string
}

func (entry Entry) String() string {
	return fmt.Sprintf("%s %s(%s)", entry.id, entry.Next.Format(time.RFC3339), entry.Prev.Format(time.RFC3339))
}

func newEntry(schedule Schedule, cmd Job) *Entry {
	entry := &Entry{
		Schedule: schedule,
		Job:      cmd,
		id:       generateUUID(time.Now()),
	}
	return entry
}

//Entries 列表
type Entries map[string]*Entry

//Sort 排序
func (entries Entries) Sort() []*Entry {
	var es = make([]*Entry, 0, len(entries))
	for _, ent := range entries {
		es = append(es, ent)
	}
	sort.Slice(es, func(i, j int) bool {
		// Two zero times should return false.
		// Otherwise, zero is "greater" than any other time.
		// (To sort it at the end of the list.)
		if es[i].Next.IsZero() {
			return false
		}
		if es[j].Next.IsZero() {
			return true
		}
		return es[i].Next.Before(es[j].Next)
	})
	return es
}

//Add 添加
func (entries *Entries) Add(ent *Entry) {
	(*entries)[ent.id] = ent
}

//Get 获取
func (entries Entries) Get(id string) (*Entry, bool) {
	ent, ok := entries[id]
	return ent, ok
}

//Remove 删除
func (entries Entries) Remove(id string) {
	delete(entries, id)
}

//Snapshot 快照
func (entries Entries) Snapshot() []*Entry {
	var ents = make([]*Entry, 0, len(entries))
	for _, e := range entries {
		ents = append(ents, &Entry{
			Schedule: e.Schedule,
			Next:     e.Next,
			Prev:     e.Prev,
			Job:      e.Job,
		})
	}
	return ents
}

// generateUUID simply generates an unique UID.
func generateUUID(seedTime time.Time) string {
	var u [16]byte
	utcTime := seedTime.In(time.UTC)
	t := uint64(utcTime.Unix()-timeBase)*10000000 + uint64(utcTime.Nanosecond()/100)

	u[0], u[1], u[2], u[3] = byte(t>>24), byte(t>>16), byte(t>>8), byte(t)
	u[4], u[5] = byte(t>>40), byte(t>>32)
	u[6], u[7] = byte(t>>56)&0x0F, byte(t>>48)

	clock := atomic.AddUint32(&clockSeq, 1)
	u[8] = byte(clock >> 8)
	u[9] = byte(clock)

	copy(u[10:], hardwareAddr)

	u[6] |= 0x10 // set version to 1 (time based uuid)
	u[8] &= 0x3F // clear variant
	u[8] |= 0x80 // set to IETF variant

	var offsets = [...]int{0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30}
	const hexString = "0123456789abcdef"
	r := make([]byte, 32)
	for i, b := range u {
		r[offsets[i]] = hexString[b>>4]
		r[offsets[i]+1] = hexString[b&0xF]
	}
	return string(r)
}
