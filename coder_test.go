package main

import (
	"runtime"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestLoadSchemaFile(t *testing.T) {
	c := qt.New(t)

	c.Run("File does not exist", func(c *qt.C) {
		coder := coder{}

		err := coder.loadSchemaFile("non-existent-path.avsc")
		c.Assert(err, qt.Not(qt.IsNil))
		c.Assert(err, qt.ErrorMatches, "failed to read AVSC file.*")
	})

	c.Run("Wrong content", func(c *qt.C) {
		coder := coder{}

		_, thisFileName, _, ok := runtime.Caller(1)
		c.Assert(ok, qt.IsTrue)
		err := coder.loadSchemaFile(thisFileName)
		c.Assert(err, qt.Not(qt.IsNil))
		c.Assert(err, qt.ErrorMatches, "failed to parse AVSC file.*")
	})

	// OK is tested through system_test.go
}
