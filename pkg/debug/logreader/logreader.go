package main

import (
	"fmt"
	"os"
	"storemy/pkg/debug/ui"
	"storemy/pkg/log/record"
	"storemy/pkg/log/wal"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type keyMap struct {
	Up     key.Binding
	Down   key.Binding
	Select key.Binding
	Back   key.Binding
	Quit   key.Binding
}

var keys = keyMap{
	Up:     ui.CommonKeys.Up,
	Down:   ui.CommonKeys.Down,
	Select: ui.CommonKeys.Select,
	Back:   ui.CommonKeys.Back,
	Quit:   ui.CommonKeys.Quit,
}

type model struct {
	records      []*record.LogRecord
	cursor       int
	selected     *record.LogRecord
	viewport     viewport.Model
	width        int
	height       int
	detailMode   bool
	err          error
	logPath      string
	totalRecords int
}

func initialModel(logPath string) model {
	return model{
		logPath: logPath,
	}
}

func (m model) Init() tea.Cmd {
	return loadRecords(m.logPath)
}

type recordsLoadedMsg struct {
	records []*record.LogRecord
	err     error
}

func loadRecords(logPath string) tea.Cmd {
	return func() tea.Msg {
		reader, err := wal.NewLogReader(logPath)
		if err != nil {
			return recordsLoadedMsg{err: err}
		}
		defer reader.Close()

		records, err := reader.ReadAll()
		if err != nil {
			return recordsLoadedMsg{err: err}
		}

		return recordsLoadedMsg{records: records}
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case recordsLoadedMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, tea.Quit
		}
		m.records = msg.records
		m.totalRecords = len(msg.records)
		return m, nil

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.viewport = viewport.New(msg.Width-4, msg.Height-10)
		return m, nil

	case tea.KeyMsg:
		if m.detailMode {
			switch {
			case key.Matches(msg, keys.Back):
				m.detailMode = false
				return m, nil
			case key.Matches(msg, keys.Quit):
				return m, tea.Quit
			}
		} else {
			switch {
			case key.Matches(msg, keys.Quit):
				return m, tea.Quit
			case key.Matches(msg, keys.Up):
				if m.cursor > 0 {
					m.cursor--
				}
			case key.Matches(msg, keys.Down):
				if m.cursor < len(m.records)-1 {
					m.cursor++
				}
			case key.Matches(msg, keys.Select):
				if m.cursor < len(m.records) {
					m.selected = m.records[m.cursor]
					m.detailMode = true
					m.viewport.SetContent(m.renderDetailView())
				}
			}
		}
	}

	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m model) View() string {
	if m.err != nil {
		return ui.ErrorStyle.Render(fmt.Sprintf("Error: %v\n\nPress q to quit.", m.err))
	}

	if len(m.records) == 0 {
		return "Loading log records...\n"
	}

	var b strings.Builder

	// Title
	title := ui.TitleStyle.Render("🗂  Database Write-Ahead Log Viewer")
	b.WriteString(title + "\n\n")

	if m.detailMode {
		// Detail view
		b.WriteString(m.viewport.View())
		b.WriteString("\n\n")
		b.WriteString(ui.HelpStyle.Render("Press esc to go back | q to quit"))
	} else {
		// List view
		b.WriteString(m.renderListView())
	}

	// Status bar
	statusBar := m.renderStatusBar()
	b.WriteString("\n" + statusBar)

	return b.String()
}

func (m model) renderListView() string {
	var b strings.Builder

	header := ui.HeaderStyle.Render(fmt.Sprintf(" Total Records: %d ", m.totalRecords))
	b.WriteString(header + "\n\n")

	visibleStart := max(0, m.cursor-10)
	visibleEnd := min(len(m.records), visibleStart+20)
	for i := visibleStart; i < visibleEnd; i++ {
		record := m.records[i]
		recordLine := m.formatRecordLine(record, i)
		if i == m.cursor {
			recordLine = ui.SelectedItemStyle.Render("▶ " + recordLine)
		} else {
			recordLine = ui.ItemStyle.Render("  " + recordLine)
		}
		b.WriteString(recordLine + "\n")
	}

	b.WriteString("\n")
	b.WriteString(ui.HelpStyle.Render("↑/↓: navigate | enter: view details | q: quit"))

	return b.String()
}

func (m model) formatRecordLine(record *record.LogRecord, index int) string {
	typeStr := m.colorizeRecordType(record.Type)

	lsnStr := ui.LabelStyle.Render("LSN:") + " " + ui.ValueStyle.Render(fmt.Sprintf("%d", record.LSN))

	tidStr := ""
	if record.TID != nil {
		tidStr = ui.LabelStyle.Render("TID:") + " " + ui.ValueStyle.Render(record.TID.String())
	}

	timeStr := lipgloss.NewStyle().Foreground(ui.MutedColor).Render(record.Timestamp.Format("15:04:05"))
	return fmt.Sprintf("[%3d] %s │ %s │ %s │ %s", index+1, typeStr, lsnStr, tidStr, timeStr)
}

func (m model) colorizeRecordType(recordType record.LogRecordType) string {
	var color lipgloss.Color
	var icon string
	var name string

	switch recordType {
	case record.BeginRecord:
		color = lipgloss.Color(ui.SuccessColor.Dark)
		icon = "▶"
		name = "BEGIN    "
	case record.CommitRecord:
		color = lipgloss.Color(ui.SuccessColor.Dark)
		icon = "✓"
		name = "COMMIT   "
	case record.AbortRecord:
		color = lipgloss.Color(ui.ErrorColor.Dark)
		icon = "✗"
		name = "ABORT    "
	case record.UpdateRecord:
		color = lipgloss.Color(ui.WarningColor.Dark)
		icon = "⟳"
		name = "UPDATE   "
	case record.InsertRecord:
		color = lipgloss.Color(ui.PrimaryColor.Dark)
		icon = "+"
		name = "INSERT   "
	case record.DeleteRecord:
		color = lipgloss.Color(ui.ErrorColor.Dark)
		icon = "−"
		name = "DELETE   "
	case record.CheckpointBegin:
		color = lipgloss.Color(ui.SecondaryColor.Dark)
		icon = "◆"
		name = "CKPT BEGIN"
	case record.CheckpointEnd:
		color = lipgloss.Color(ui.SecondaryColor.Dark)
		icon = "◇"
		name = "CKPT END  "
	case record.CLRRecord:
		color = lipgloss.Color(ui.MutedColor.Dark)
		icon = "↶"
		name = "CLR      "
	default:
		color = lipgloss.Color(ui.MutedColor.Dark)
		icon = "?"
		name = "UNKNOWN  "
	}

	return lipgloss.NewStyle().Foreground(color).Render(icon + " " + name)
}

func (m model) renderDetailView() string {
	if m.selected == nil {
		return "No record selected"
	}

	var b strings.Builder

	re := m.selected

	typeStr := m.colorizeRecordType(re.Type)
	b.WriteString(lipgloss.NewStyle().Bold(true).Foreground(ui.PrimaryColor).Render("Record Details") + "\n\n")
	b.WriteString(ui.LabelStyle.Render("Type: ") + typeStr + "\n\n")

	b.WriteString(m.renderKeyValue("LSN", fmt.Sprintf("%d", re.LSN)))
	b.WriteString(m.renderKeyValue("Previous LSN", fmt.Sprintf("%d", re.PrevLSN)))
	b.WriteString(m.renderKeyValue("Timestamp", re.Timestamp.Format("2006-01-02 15:04:05")))

	if re.TID != nil {
		b.WriteString(m.renderKeyValue("Transaction ID", re.TID.String()))
	}

	b.WriteString("\n")

	// Type-specific details
	switch re.Type {
	case record.UpdateRecord, record.InsertRecord, record.DeleteRecord:
		if re.PageID != nil {
			b.WriteString(ui.LabelStyle.Render("Page Information:") + "\n")
			b.WriteString(m.renderKeyValue("  Table ID", fmt.Sprintf("%d", re.PageID.GetTableID())))
			b.WriteString(m.renderKeyValue("  Page Number", fmt.Sprintf("%d", re.PageID.PageNo())))
			b.WriteString("\n")
		}

		if re.BeforeImage != nil {
			b.WriteString(m.renderKeyValue("Before Image Size", fmt.Sprintf("%d bytes", len(re.BeforeImage))))
		}
		if re.AfterImage != nil {
			b.WriteString(m.renderKeyValue("After Image Size", fmt.Sprintf("%d bytes", len(re.AfterImage))))
		}

	case record.CLRRecord:
		if re.PageID != nil {
			b.WriteString(ui.LabelStyle.Render("CLR Information:") + "\n")
			b.WriteString(m.renderKeyValue("  Undo Next LSN", fmt.Sprintf("%d", re.UndoNextLSN)))
			b.WriteString(m.renderKeyValue("  Table ID", fmt.Sprintf("%d", re.PageID.GetTableID())))
			b.WriteString(m.renderKeyValue("  Page Number", fmt.Sprintf("%d", re.PageID.PageNo())))
		}
	}

	return ui.DetailStyle.Render(b.String())
}

func (m model) renderKeyValue(key, value string) string {
	return fmt.Sprintf("%s %s\n",
		ui.LabelStyle.Render(key+":"),
		ui.ValueStyle.Render(value))
}

func (m model) renderStatusBar() string {
	position := fmt.Sprintf("%d/%d", m.cursor+1, m.totalRecords)

	if m.detailMode {
		return ui.StatusBarStyle.Render(fmt.Sprintf(" Detail View | Position: %s ", position))
	}

	return ui.StatusBarStyle.Render(fmt.Sprintf(" List View | Position: %s | %s ", position, m.logPath))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: logreader <path-to-log-file>")
		os.Exit(1)
	}

	logPath := os.Args[1]

	p := tea.NewProgram(
		initialModel(logPath),
		tea.WithAltScreen(),
	)

	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
