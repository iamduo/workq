package prot

// Protocol Command Data Structure
// A cmd holds all the data required for processing. Is not gauranteed to be a
// completely valid command. Validity is up to each command processor.
type Cmd struct {
	Name  string
	ArgC  int
	Args  [][]byte
	Flags CmdFlags
	FlagC int
}

func NewCmd(name string, args [][]byte, flags CmdFlags) *Cmd {
	return &Cmd{
		Name:  name,
		ArgC:  len(args),
		Args:  args,
		Flags: flags,
		FlagC: len(flags),
	}
}

type CmdFlags map[string][]byte
