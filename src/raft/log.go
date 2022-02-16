package raft

import (
	"fmt"
	"strings"
)

// 日志条目
type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

// 包含所有日志条目和对应索引
type Log struct {
	Entries []Entry
	Index0  int
}

// append日志内容
func (l *Log) append(entries ...Entry) {
	l.Entries = append(l.Entries, entries...)
}

// 创建一个新的Log
func makeEntryLog() Log {
	log := Log{
		Entries: make([]Entry, 0),
		Index0:  0,
	}
	return log
}

// 根据日志条目的索引获取对应日志条目
func (l *Log) at(idx int) *Entry {
	return &l.Entries[idx]
}

// 只保存Log的[:idx-1]范围的日志条目，其他删除
func (l *Log) truncate(idx int) {
	// 基于前idx个元素创建切片
	l.Entries = l.Entries[:idx]
}

// 获取[idx-1:]范围的日志条目并返回
func (l *Log) slice(idx int) []Entry {
	// 基于第idx个元素创建切片
	return l.Entries[idx:]
}

func (l *Log) len() int {
	return len(l.Entries)
}

func (l *Log) lastLog() *Entry {
	return l.at(len(l.Entries) - 1)
}

// 返回字符串形式的某条日志条目的任期Term
func (e *Entry) String() string {
	return fmt.Sprint(e.Term)
}

// 返回字符串形式的所有日志条目的任期Term
func (l *Log) String() string {
	nums := []string{}
	for _, entry := range l.Entries {
		nums = append(nums, fmt.Sprintf("%4d", entry.Term))
	}
	return fmt.Sprintf(strings.Join(nums, "|"))
}

func makeEmptyLog() Log {
	log := Log{
		Entries: make([]Entry, 0),
		Index0:  0,
	}
	return log
}
