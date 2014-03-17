package heka_disqus_plugins

import (
	"fmt"
	"github.com/mozilla-services/heka/pipeline"
	"io"
	"os/exec"
)

const NEWLINE byte = '\n'

// PipeOutput is an output plugin to pipe into an executable
type PipeOutput struct {
	path  string
	args  []string
	cmd   *exec.Cmd
	stdin io.Writer
}

// PipeOutputConfig is the ConfigStruct for PipeOutput
type PipeOutputConfig struct {
	// Path to executable
	Path string

	// Args to pass to command
	Args []string
}

func (o *PipeOutput) ConfigStruct() interface{} {
	return &PipeOutputConfig{}
}

func (o *PipeOutput) Init(config interface{}) (err error) {
	conf := config.(*PipeOutputConfig)

	o.path, err = exec.LookPath(conf.Path)
	if err != nil {
		err = fmt.Errorf("PipeOuput '%s' error locating path: %s", o.path, err)
		return err
	}

	o.args = conf.Args

	o.cmd = exec.Command(o.path, o.args...)

	o.stdin, err = o.cmd.StdinPipe()
	if err != nil {
		err = fmt.Errorf("PipeOuput '%s' error: %s", o.path, err)
		return err
	}

	err = o.cmd.Start()
	if err != nil {
		err = fmt.Errorf("PipeOuput '%s' error starting command: %s", o.path, err)
		return err
	}

	return
}

func (o *PipeOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) (err error) {
	var (
		n    int
		pack *pipeline.PipelinePack
	)
	outBytes := make([]byte, 0, 8192)

	inChan := or.InChan()

	for pack = range inChan {
		outBytes = outBytes[:0]

		outBytes = append(outBytes, *pack.Message.Payload...)
		outBytes = append(outBytes, NEWLINE)

		pack.Recycle()

		n, err = o.stdin.Write(outBytes)
		if err != nil {
			or.LogError(fmt.Errorf("Can't pipe to '%s': %s", o.path, err))
			break
		} else if n != len(outBytes) {
			or.LogError(fmt.Errorf("Truncated output to '%s'", o.path))
		}
	}

	err = o.cmd.Wait()
	or.LogError(fmt.Errorf("OutputPipe '%s' exited: %s", o.path, err))

	return
}

func init() {
	pipeline.RegisterPlugin("PipeOutput", func() interface{} {
		return &PipeOutput{}
	})
}
