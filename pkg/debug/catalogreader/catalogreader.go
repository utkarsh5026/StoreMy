package main

import (
	"fmt"
	"os"
	"storemy/pkg/catalog"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/debug/ui"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/types"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	catalogKeys = ui.CommonKeys
	statsTable  = systemtable.Stats.TableName()
	colsTable   = systemtable.Columns.TableName()
	tablesTable = systemtable.Tables.TableName()
)

type catalogModel struct {
	catalog       *catalog.CatalogManager
	store         *memory.PageStore
	currentView   string // "menu", "tables", "columns", "statistics"
	cursor        int
	viewport      viewport.Model
	width         int
	height        int
	err           error
	dataDir       string
	tableData     [][]string
	columnHeaders []string
}

func initialCatalogModel(dataDir string) catalogModel {
	return catalogModel{
		dataDir:     dataDir,
		currentView: "menu",
	}
}

func (m catalogModel) Init() tea.Cmd {
	return initializeCatalog(m.dataDir)
}

type catalogInitMsg struct {
	catalog *catalog.CatalogManager
	store   *memory.PageStore
	err     error
}

func initializeCatalog(dataDir string) tea.Cmd {
	return func() tea.Msg {
		store := memory.NewPageStore(nil) // No WAL for reading
		cat := catalog.NewCatalogManager(store, dataDir)

		tx := transaction.NewTransactionContext(primitives.NewTransactionID())
		if err := cat.Initialize(tx); err != nil {
			return catalogInitMsg{err: err}
		}

		if err := cat.LoadAllTables(transaction.NewTransactionContext(primitives.NewTransactionID())); err != nil {
			return catalogInitMsg{err: err}
		}

		return catalogInitMsg{
			catalog: cat,
			store:   store,
		}
	}
}

type tableLoadedMsg struct {
	headers []string
	data    [][]string
	err     error
}

func loadTableData(cat *catalog.CatalogManager, tableName string) tea.Cmd {
	return func() tea.Msg {
		var err error
		tid := primitives.NewTransactionID()
		tableID, err := cat.GetTableID(tid, tableName)
		if err != nil {
			return tableLoadedMsg{err: err}
		}
		file, err := cat.GetTableFile(tableID)
		if err != nil {
			return tableLoadedMsg{err: err}
		}

		iter := file.Iterator(tid)
		if err := iter.Open(); err != nil {
			return tableLoadedMsg{err: err}
		}
		defer iter.Close()

		schema := file.GetTupleDesc()
		headers := make([]string, schema.NumFields())
		for i := 0; i < schema.NumFields(); i++ {
			name, _ := schema.GetFieldName(i)
			headers[i] = name
		}

		var data [][]string
		for {
			hasNext, err := iter.HasNext()
			if err != nil || !hasNext {
				break
			}

			tup, err := iter.Next()
			if err != nil || tup == nil {
				break
			}

			row := make([]string, schema.NumFields())
			for i := 0; i < schema.NumFields(); i++ {
				field, _ := tup.GetField(i)
				row[i] = formatField(field)
			}
			data = append(data, row)
		}

		return tableLoadedMsg{
			headers: headers,
			data:    data,
		}
	}
}

func formatField(field types.Field) string {
	if field == nil {
		return "NULL"
	}
	switch f := field.(type) {
	case *types.IntField:
		return fmt.Sprintf("%d", f.Value)
	case *types.StringField:
		return strings.TrimSpace(f.Value)
	case *types.BoolField:
		return fmt.Sprintf("%t", f.Value)
	default:
		return fmt.Sprintf("%v", field)
	}
}

func (m catalogModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case catalogInitMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, tea.Quit
		}
		m.catalog = msg.catalog
		m.store = msg.store
		return m, nil

	case tableLoadedMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.columnHeaders = msg.headers
		m.tableData = msg.data
		m.cursor = 0
		return m, nil

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.viewport = viewport.New(msg.Width-4, msg.Height-10)
		return m, nil

	case tea.KeyMsg:
		switch m.currentView {
		case "menu":
			switch {
			case key.Matches(msg, catalogKeys.Quit):
				return m, tea.Quit
			case key.Matches(msg, catalogKeys.Up):
				if m.cursor > 0 {
					m.cursor--
				}
			case key.Matches(msg, catalogKeys.Down):
				if m.cursor < 2 {
					m.cursor++
				}
			case key.Matches(msg, catalogKeys.Select):
				if m.catalog == nil {
					return m, nil
				}
				tables := []string{tablesTable, colsTable, statsTable}
				m.currentView = tables[m.cursor]
				return m, loadTableData(m.catalog, m.currentView)
			}

		case tablesTable, colsTable, statsTable:
			switch {
			case key.Matches(msg, catalogKeys.Quit):
				return m, tea.Quit
			case key.Matches(msg, catalogKeys.Back):
				m.currentView = "menu"
				m.cursor = 0
				m.tableData = nil
				m.columnHeaders = nil
				return m, nil
			case key.Matches(msg, catalogKeys.Up):
				if m.cursor > 0 {
					m.cursor--
				}
			case key.Matches(msg, catalogKeys.Down):
				if m.cursor < len(m.tableData)-1 {
					m.cursor++
				}
			}
		}
	}

	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m catalogModel) View() string {
	if m.err != nil {
		return ui.ErrorStyle.Render(fmt.Sprintf("Error: %v\n\nPress q to quit.", m.err))
	}

	if m.catalog == nil {
		return "Initializing catalog...\n"
	}

	var b strings.Builder

	title := ui.TitleStyle.Render("🗄  System Catalog Viewer")
	b.WriteString(title + "\n\n")

	switch m.currentView {
	case "menu":
		b.WriteString(m.renderMenu())
	case tablesTable, colsTable, statsTable:
		b.WriteString(m.renderTableView())
	}

	statusBar := m.renderStatusBar()
	b.WriteString("\n" + statusBar)

	return b.String()
}

func (m catalogModel) renderMenu() string {
	var b strings.Builder

	header := ui.HeaderStyle.Render(" Select a Catalog Table ")
	b.WriteString(header + "\n\n")

	tables := []string{
		"CATALOG_TABLES - Table Metadata",
		"CATALOG_COLUMNS - Column Definitions",
		"CATALOG_STATISTICS - Table Statistics",
	}

	for i, table := range tables {
		if i == m.cursor {
			b.WriteString(ui.SelectedItemStyle.Render("▶ "+table) + "\n")
		} else {
			b.WriteString(ui.ItemStyle.Render("  "+table) + "\n")
		}
	}

	b.WriteString("\n")
	b.WriteString(ui.HelpStyle.Render("↑/↓: navigate | enter: select | q: quit"))

	return b.String()
}

func (m catalogModel) renderTableView() string {
	if len(m.tableData) == 0 {
		return "No data in this table.\n\n" + ui.HelpStyle.Render("Press esc to go back | q to quit")
	}

	var b strings.Builder

	// Table title
	tableTitle := ui.HeaderStyle.Render(fmt.Sprintf(" %s (%d rows) ", m.currentView, len(m.tableData)))
	b.WriteString(tableTitle + "\n\n")

	// Calculate column widths
	colWidths := make([]int, len(m.columnHeaders))
	for i, header := range m.columnHeaders {
		colWidths[i] = len(header)
	}
	for _, row := range m.tableData {
		for i, cell := range row {
			if len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	// Render headers
	headerRow := ""
	for i, header := range m.columnHeaders {
		headerRow += ui.TableHeaderStyle.Render(padString(header, colWidths[i]))
		if i < len(m.columnHeaders)-1 {
			headerRow += " "
		}
	}
	b.WriteString(headerRow + "\n")

	// Render separator
	separator := ""
	for i, width := range colWidths {
		separator += strings.Repeat("─", width+2)
		if i < len(colWidths)-1 {
			separator += "┼"
		}
	}
	b.WriteString(lipgloss.NewStyle().Foreground(ui.MutedColor).Render(separator) + "\n")

	// Render data rows
	visibleStart := max(0, m.cursor-10)
	visibleEnd := min(len(m.tableData), visibleStart+20)

	for i := visibleStart; i < visibleEnd; i++ {
		row := m.tableData[i]
		rowStr := ""
		for j, cell := range row {
			cellContent := padString(cell, colWidths[j])
			if i == m.cursor {
				rowStr += ui.SelectedItemStyle.Render(cellContent)
			} else {
				rowStr += ui.CellStyle.Render(cellContent)
			}
			if j < len(row)-1 {
				rowStr += " "
			}
		}
		b.WriteString(rowStr + "\n")
	}

	b.WriteString("\n")
	b.WriteString(ui.HelpStyle.Render("↑/↓: navigate | esc: back to menu | q: quit"))

	return b.String()
}

func (m catalogModel) renderStatusBar() string {
	var status string
	if m.currentView == "menu" {
		status = fmt.Sprintf(" Menu | Data Directory: %s ", m.dataDir)
	} else {
		position := fmt.Sprintf("%d/%d", m.cursor+1, len(m.tableData))
		status = fmt.Sprintf(" %s | Position: %s ", m.currentView, position)
	}
	return ui.StatusBarStyle.Render(status)
}

func padString(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: catalogreader <data-directory>")
		os.Exit(1)
	}

	dataDir := os.Args[1]

	p := tea.NewProgram(
		initialCatalogModel(dataDir),
		tea.WithAltScreen(),
	)

	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
