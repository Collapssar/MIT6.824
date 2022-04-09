package main

import (
	"github.com/Collapssar/mapreduce/mr"
	"strconv"
	"strings"
	"unicode"
)

func Map(filename string, contents string) []mr.KeyValue {
	// detect word separators
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	// split contents into a slice of words.
	words := strings.FieldsFunc(contents, ff)


	var kvs []mr.KeyValue
	for _, w := range words {
		kv := mr.KeyValue{Key: w, Value: "1"}
		kvs = append(kvs, kv)
	}
	return kvs
}

func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}