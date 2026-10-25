package tui

import (
	"strings"

	"charm.land/bubbles/v2/key"
)

// keyMap follows k9s: / search, f filter, s sort, n namespace, c context, Enter, Esc back, ? help, : commands,
// q quit.
type keyMap struct {
	up, down, pageUp, pageDown, top, bottom                 key.Binding
	search, filter, sort, namespace, context, command, help key.Binding
	enter, back, quit, forceQuit                            key.Binding
}

func newKeyMap() keyMap {
	b := func(help, desc string, keys ...string) key.Binding {
		return key.NewBinding(key.WithKeys(keys...), key.WithHelp(help, desc))
	}
	return keyMap{
		up:        b("↑/k", "up", "up", "k"),
		down:      b("↓/j", "down", "down", "j"),
		pageUp:    b("pgup/ctrl+b", "page up", "pgup", "ctrl+b"),
		pageDown:  b("pgdn/ctrl+f", "page down", "pgdown", "ctrl+f"),
		top:       b("g/home", "first job", "g", "home"),
		bottom:    b("G/end", "last job", "G", "end"),
		search:    b("/", "search name or ID label", "/"),
		filter:    b("f", "health filter", "f"),
		sort:      b("s", "sort", "s"),
		namespace: b("n", "namespace", "n"),
		context:   b("c", "context", "c"),
		command:   b(":", "command", ":"),
		help:      b("?", "help", "?"),
		enter:     b("enter", "details", "enter"),
		back:      b("esc", "back, clear filters", "esc"),
		quit:      b("q", "quit", "q"),
		forceQuit: b("ctrl+c", "quit", "ctrl+c"),
	}
}

// short is the footer: the keys a first-time user needs.
func (k *keyMap) short() string {
	bindings := []key.Binding{k.enter, k.search, k.filter, k.sort, k.namespace, k.context, k.command, k.help, k.quit}
	parts := make([]string, 0, len(bindings))
	for _, b := range bindings {
		parts = append(parts, b.Help().Key+" "+b.Help().Desc)
	}
	return strings.Join(parts, "  ")
}

// full is the help screen, in groups.
func (k *keyMap) full() [][]key.Binding {
	return [][]key.Binding{
		{k.up, k.down, k.pageUp, k.pageDown, k.top, k.bottom},
		{k.search, k.filter, k.sort, k.namespace, k.context, k.enter},
		{k.command, k.back, k.help, k.quit},
	}
}
