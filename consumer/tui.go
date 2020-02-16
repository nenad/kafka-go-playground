package main

import (
	"context"
	"fmt"
	"time"

	"github.com/mum4k/termdash"
	"github.com/mum4k/termdash/align"
	"github.com/mum4k/termdash/cell"
	"github.com/mum4k/termdash/container"
	"github.com/mum4k/termdash/linestyle"
	"github.com/mum4k/termdash/terminal/termbox"
	"github.com/mum4k/termdash/terminal/terminalapi"
	"github.com/mum4k/termdash/widgets/button"
	"github.com/mum4k/termdash/widgets/segmentdisplay"
	"github.com/mum4k/termdash/widgets/text"
)

type KUI struct {
	display *segmentdisplay.SegmentDisplay
	log     *text.Text
}

type KLogger interface {
	Printf(format string, v ...interface{})
}

func NewKUI() (KUI, error) {
	display, err := segmentdisplay.New()
	if err != nil {
		return KUI{}, err
	}

	log, err := text.New(text.RollContent())
	if err != nil {
		return KUI{}, err
	}

	return KUI{
		display: display,
		log:     log,
	}, nil
}

func (k *KUI) Write(text string) error {
	return k.display.Write([]*segmentdisplay.TextChunk{segmentdisplay.NewChunk(text)})
}

func (k *KUI) WriteLog(text string) error {
	return k.log.Write(text)
}

func (k *KUI) Printf(format string, v ...interface{}) {
	k.log.Write(fmt.Sprintf(format, v...))
}

func (k *KUI) Run(ctx context.Context, onAdd, onSub func(display *segmentdisplay.SegmentDisplay) error) error {
	ctx, cancel := context.WithCancel(ctx)

	t, err := termbox.New()
	if err != nil {
		return err
	}
	defer t.Close()

	if err := k.display.Write([]*segmentdisplay.TextChunk{segmentdisplay.NewChunk("none"),}); err != nil {
		return err
	}

	addB, err := button.New("(a)dd", func() error {
		return onAdd(k.display)
	}, button.FillColor(cell.ColorGreen), button.GlobalKey('a'), button.Width(20))
	if err != nil {
		return err
	}

	subB, err := button.New("(s)ub", func() error {
		return onSub(k.display)
	}, button.FillColor(cell.ColorRed), button.GlobalKey('s'), button.Width(20))
	if err != nil {
		return err
	}

	c, err := container.New(t,
		container.Border(linestyle.Light),
		container.BorderTitle("Press Q to exit"),
		container.SplitHorizontal(
			container.Top(
				container.SplitVertical(
					container.Left(container.PlaceWidget(k.display), container.AlignHorizontal(align.HorizontalCenter)),
					container.Right(container.PlaceWidget(k.log), container.AlignHorizontal(align.HorizontalCenter)),
					container.SplitPercent(20),
				),
			),
			container.Bottom(
				container.SplitVertical(
					container.Left(container.PlaceWidget(addB), container.AlignHorizontal(align.HorizontalRight)),
					container.Right(container.PlaceWidget(subB), container.AlignHorizontal(align.HorizontalLeft)),
					container.SplitPercent(50),
				),
			),
			container.SplitPercent(80),
		),
	)
	if err != nil {
		return err
	}

	quitter := func(kb *terminalapi.Keyboard) {
		if kb.Key == 'q' || kb.Key == 'Q' {
			cancel()
		}
	}

	return termdash.Run(ctx, t, c, termdash.KeyboardSubscriber(quitter), termdash.RedrawInterval(time.Millisecond*10))
}
