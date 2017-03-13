package prot

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"github.com/iamduo/workq/int/job"
)

func argsFromString(args string) [][]byte {
	return bytes.Split([]byte(args), []byte(" "))
}

func TestParseValidCommands(t *testing.T) {
	tests := []struct {
		in  []byte
		cmd *Cmd
	}{
		{
			[]byte("add 123 q1 10 100 0\r\n\r\n"),
			NewCmd("add", argsFromString("123 q1 10 100 0"), make(CmdFlags)),
		},
		{
			[]byte("add 123 q1 10 100 1\r\na\r\n"),
			NewCmd("add", argsFromString("123 q1 10 100 1 a"), make(CmdFlags)),
		},
		{
			[]byte("add 123 q1 10 100 1 -max-attempts=1\r\na\r\n"),
			NewCmd("add", argsFromString("123 q1 10 100 1 a"), CmdFlags{"max-attempts": []byte("1")}),
		},
		{
			[]byte("add -max-attempts=1 123 q1 10 100 1\r\na\r\n"),
			NewCmd("add", argsFromString("123 q1 10 100 1 a"), CmdFlags{"max-attempts": []byte("1")}),
		},
		{
			[]byte("complete 123 0\r\n\r\n"),
			NewCmd("complete", argsFromString("123 0"), make(CmdFlags)),
		},
		{
			[]byte("complete 123 1\r\na\r\n"),
			NewCmd("complete", argsFromString("123 1"), make(CmdFlags)),
		},
		{
			[]byte("delete 123\r\n\r\n"),
			NewCmd("delete", argsFromString("123"), make(CmdFlags)),
		},
		{
			[]byte("fail 123 0\r\n\r\n"),
			NewCmd("fail", argsFromString("123 0"), make(CmdFlags)),
		},
		{
			[]byte("fail 123 1\r\na\r\n"),
			NewCmd("fail", argsFromString("123 1"), make(CmdFlags)),
		},
		{
			[]byte("lease q1 123\r\n\r\n"),
			NewCmd("lease", argsFromString("q1 123"), make(CmdFlags)),
		},
		{
			[]byte("lease q1 q2 123\r\n\r\n"),
			NewCmd("lease", argsFromString("q1 q2 123"), make(CmdFlags)),
		},
		{
			[]byte("lease q1 q2 q3 123\r\n\r\n"),
			NewCmd("lease", argsFromString("q1 q2 q3 123"), make(CmdFlags)),
		},
		{
			[]byte("run 123 q1 10 100 0\r\n\r\n"),
			NewCmd("run", argsFromString("123 q1 10 100 0"), make(CmdFlags)),
		},
		{
			[]byte("run 123 q1 10 100 1\r\na\r\n"),
			NewCmd("run", argsFromString("123 q1 10 100 1 a"), make(CmdFlags)),
		},
		{
			[]byte("run 123 q1 10 100 1\r\na\r\n"),
			NewCmd("run", argsFromString("123 q1 10 100 1 a"), make(CmdFlags)),
		},
		{
			[]byte("schedule 123 q1 10 100 2016-06-13T14:08:18Z 0\r\n\r\n"),
			NewCmd("schedule", argsFromString("123 q1 10 100 2016-06-13T14:08:18Z 0"), make(CmdFlags)),
		},
		{
			[]byte("schedule 123 q1 10 100 2016-06-13T14:08:18Z 1\r\na\r\n"),
			NewCmd("schedule", argsFromString("123 q1 10 100 2016-06-13T14:08:18Z 1 a"), make(CmdFlags)),
		},
		{
			[]byte("schedule 123 q1 10 100 2016-06-13T14:08:18Z 1\r\na\r\n"),
			NewCmd("schedule", argsFromString("123 q1 10 100 2016-06-13T14:08:18Z 1 a"), make(CmdFlags)),
		},
		{
			[]byte("schedule 123 q1 10 100 2016-06-13T14:08:18Z 1 -max-attempts=1 -max-fails=1\r\na\r\n"),
			NewCmd(
				"schedule",
				argsFromString("123 q1 10 100 2016-06-13T14:08:18Z 1 a"),
				CmdFlags{"max-attempts": []byte("1"), "max-fails": []byte("1")},
			),
		},
		{
			[]byte("result 123 1\r\n\r\n"),
			NewCmd("result", argsFromString("123 1"), make(CmdFlags)),
		},
	}

	prot := &Prot{}
	for _, data := range tests {
		cmd, err := prot.ParseCmd(bufio.NewReader(bytes.NewReader(data.in)))
		if err != nil || cmd == nil {
			t.Fatalf("err=%v, cmd=%v", err, cmd)
		}

		if data.cmd.Name != cmd.Name {
			t.Fatalf("expected=%v, actual=%v", data.cmd.Name, cmd.Name)
		}

		for i, _ := range data.cmd.Args {
			if !bytes.Equal(data.cmd.Args[i], cmd.Args[i]) {
				t.Fatalf("Args: expected=%s, actual=%s", data.cmd.Args[i], cmd.Args[i])
			}
		}

		for k, _ := range data.cmd.Flags {
			if !bytes.Equal(data.cmd.Flags[k], cmd.Flags[k]) {
				t.Fatalf("Flags: key=%s, expected=%s, actual=%s", k, data.cmd.Flags[k], cmd.Flags[k])
			}
		}
	}
}

func TestParseInvalidInput(t *testing.T) {
	tests := []struct {
		in  []byte
		err error
	}{
		// Common Invalid Tests

		{[]byte(""), ErrReadErr},
		{[]byte("does-not-exist"), ErrReadErr},
		{[]byte("\n"), ErrReadErr},
		{[]byte("\r\n"), ErrUnknownCmd},
		{[]byte("\r\n\r\n"), ErrUnknownCmd},
		{[]byte("-max-attempts=1 add 123 q1 10 100 1\r\na\r\n"), ErrUnknownCmd},
		{[]byte("add\r"), ErrReadErr},
		{[]byte("add\n"), ErrReadErr},
		{[]byte("add -max-attempts= add 123 q1 10 100 1\r\na\r\n"), ErrInvalidCmdFlags},
		{[]byte("add -max-attempts=1 -max-attempts=1 add 123 q1 10 100 1\r\na\r\n"), ErrInvalidCmdFlags},

		// Command Specific

		{[]byte("add\n"), ErrReadErr},
		{[]byte("add\r\n"), ErrInvalidCmdArgs},
		{[]byte("add 123 q1 10 100\r\na\r\n"), ErrInvalidCmdArgs},
		{[]byte("add 123 q1 10 100 1 -max-attempts\r\na\r\n"), ErrInvalidCmdArgs},
		{[]byte("add     -8\r\n"), ErrInvalidCmdArgs},
		{[]byte("add 123 q1 10 100 1\r\n"), ErrInvalidPayloadSize},
		{[]byte("add 123 q1 10 100 3\r\nabc"), ErrInvalidPayloadSize},
		{[]byte("add 123 q1 10 100 1\r\nab\r\n"), ErrInvalidPayloadSize},
		{[]byte("add 123 q1 10 100 2\r\nab\n"), ErrInvalidPayloadSize},
		{[]byte("add 123 q1 10 100 3\r\nab\n"), ErrInvalidPayloadSize},
		{[]byte("add 123 q1 10 100 *\r\nab\r\n"), ErrInvalidPayloadSize},
		{[]byte("add 123 q1 10 100 -8\r\nab\r\n"), ErrInvalidPayloadSize},

		{[]byte("complete\r\n"), ErrInvalidCmdArgs},
		{[]byte("complete 1\r\n"), ErrInvalidCmdArgs},
		{[]byte("complete 1 *\r\n"), ErrInvalidPayloadSize},
		{[]byte("complete 1 -1\r\n"), ErrInvalidPayloadSize},
		{[]byte("complete 1 1\r\n\r\n"), ErrInvalidPayloadSize},
		{[]byte("complete 1 0\r\na\r\n"), ErrInvalidPayloadSize},

		{[]byte("delete\r\n"), ErrInvalidCmdArgs},

		{[]byte("fail\r\n"), ErrInvalidCmdArgs},
		{[]byte("fail 1\r\n"), ErrInvalidCmdArgs},
		{[]byte("fail 1 *\r\n"), ErrInvalidResultSize},
		{[]byte("fail 1 -1\r\n"), ErrInvalidResultSize},
		{[]byte("fail 1 1\r\n\r\n"), ErrInvalidResultSize},
		{[]byte("fail 1 0\r\na\r\n"), ErrInvalidResultSize},

		{[]byte("lease\r\n"), ErrInvalidCmdArgs},
		{[]byte("lease q1\r\n"), ErrInvalidCmdArgs},

		{[]byte("run\r\n"), ErrInvalidCmdArgs},
		{[]byte("run 123\r\n"), ErrInvalidCmdArgs},
		{[]byte("run 123 q1\r\n"), ErrInvalidCmdArgs},
		{[]byte("run 123 q1 10\r\n"), ErrInvalidCmdArgs},
		{[]byte("run 123 q1 10 1000\r\n"), ErrInvalidCmdArgs},
		{[]byte("run 123 q1 10 1000 1\r\n"), ErrInvalidPayloadSize},
		{[]byte("run 123 q1 10 1000 1\r\na"), ErrInvalidPayloadSize},
		{[]byte("run 123 q1 10 1000 1\r\nab\r\n"), ErrInvalidPayloadSize},
		{[]byte("run 123 q1 10 1000 -1\r\na\r\n"), ErrInvalidPayloadSize},
		{[]byte("run 123 q1 10 1000 *\r\na\r\n"), ErrInvalidPayloadSize},

		{[]byte("schedule\r\n"), ErrInvalidCmdArgs},
		{[]byte("schedule 123\r\n"), ErrInvalidCmdArgs},
		{[]byte("schedule 123 q1\r\n"), ErrInvalidCmdArgs},
		{[]byte("schedule 123 q1 10\r\n"), ErrInvalidCmdArgs},
		{[]byte("schedule 123 q1 10 1000\r\n"), ErrInvalidCmdArgs},
		{[]byte("schedule 123 q1 10 1000 2016-06-13T14:08:18Z\r\n"), ErrInvalidCmdArgs},
		{[]byte("schedule 123 q1 10 1000 2016-06-13T14:08:18Z 1\r\n"), ErrInvalidPayloadSize},
		{[]byte("schedule 123 q1 10 1000 2016-06-13T14:08:18Z 1\r\na"), ErrInvalidPayloadSize},
		{[]byte("schedule 123 q1 10 1000 2016-06-13T14:08:18Z 1\r\nab\r\n"), ErrInvalidPayloadSize},
		{[]byte("schedule 123 q1 10 1000 2016-06-13T14:08:18Z -1\r\na\r\n"), ErrInvalidPayloadSize},
		{[]byte("schedule 123 q1 10 1000 2016-06-13T14:08:18Z *\r\na\r\n"), ErrInvalidPayloadSize},

		{[]byte("result\r\n"), ErrInvalidCmdArgs},
		{[]byte("result 123\r\n"), ErrInvalidCmdArgs},
	}

	prot := &Prot{}
	for i, data := range tests {
		cmd, err := prot.ParseCmd(bufio.NewReader(bytes.NewReader(data.in)))
		if data.err != err {
			t.Errorf("index=%v, in=%s, expected=%v, actual=%v, cmd=%v", i, data.in, data.err, err, cmd)
		}

		if cmd != nil {
			t.Fail()
		}
	}
}

func TestSendReply(t *testing.T) {
	var buf bytes.Buffer
	prot := &Prot{}
	b := []byte("a")
	prot.SendReply(&buf, b)

	exp := []byte{'+'}
	exp = append(exp, b...)
	if !bytes.Equal(exp, buf.Bytes()) {
		t.Fatalf("SendReply mismatch, exp=%+v, act=%+v", exp, buf.Bytes())
	}
}

func TestSendErr(t *testing.T) {
	var buf bytes.Buffer
	prot := &Prot{}
	b := "error"
	prot.SendErr(&buf, b)

	exp := []byte{'-'}
	exp = append(exp, b...)
	if !bytes.Equal(exp, buf.Bytes()) {
		t.Fatalf("SendReply mismatch, exp=%s, act=%s", exp, buf.Bytes())
	}
}

func TestOkJobResp(t *testing.T) {
	var id job.ID
	name := "a"
	payload := []byte("p")
	expResp := []byte(fmt.Sprintf("OK 1\r\n%s %s %d\r\n%s\r\n", id, name, 1, payload))
	resp := OkJobResp(id, name, payload)
	if !bytes.Equal(expResp, resp) {
		t.Fatalf("OkJobResp mismatch, exp=%+v, act=%+v", expResp, resp)
	}
}

func TestOkResultResp(t *testing.T) {
	var id job.ID
	payload := []byte("p")
	expResp := []byte(fmt.Sprintf("OK 1\r\n%s 1 %d\r\n%s\r\n", id, 1, payload))
	resp := OkResultResp(id, true, payload)
	if !bytes.Equal(expResp, resp) {
		t.Fatalf("OkJobResp mismatch, exp=%+v, act=%+v", expResp, resp)
	}

	expResp = []byte(fmt.Sprintf("OK 1\r\n%s 0 %d\r\n%s\r\n", id, 1, payload))
	resp = OkResultResp(id, false, payload)
	if !bytes.Equal(expResp, resp) {
		t.Fatalf("OkJobResp mismatch, exp=%+v, act=%+v", expResp, resp)
	}
}

func TestOkResp(t *testing.T) {
	if !bytes.Equal([]byte("OK\r\n"), OkResp()) {
		t.Fatalf("OkResp mismatch")
	}
}

func BenchmarkParseCmd(b *testing.B) {
	// add 6ba7b810-9dad-11d1-80b4-00c04fd430c4 ping 5000 60000 100\r\n
	// <payload-bytes>\r\n
	cmd := []byte("add 6ba7b810-9dad-11d1-80b4-00c04fd430c4 ping 5000 60000 100\r\n")
	cmd = append(cmd, make([]byte, 100)...)
	cmd = append(cmd, []byte("\r\n")...)
	r := bytes.NewReader(cmd)
	bufR := bufio.NewReader(r)

	p := Prot{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		_, err := p.ParseCmd(bufR)
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}

		r.Reset(cmd)
	}
}

/*func TestParseFlags(t *testing.T) {
	tests := []struct {
		in    [][]byte
		flags CmdFlags
	}{
		{argsFromString("add 123 -max-attempts=1"), CmdFlags{"max-attempts": []byte("1")}},
		{argsFromString("add -max-attempts=1 123"), CmdFlags{"max-attempts": []byte("1")}},
	}

	for _, data := range tests {
		flags, err := parseFlags(data.in)
		if err != nil {
			t.FailNow()
		}

		for k, v := range data.flags {
			vv, ok := flags[k]
			if !ok {
				t.FailNow()
			}

			if !bytes.Equal(v, vv) {
				t.FailNow()
			}

		}
	}
}*/
