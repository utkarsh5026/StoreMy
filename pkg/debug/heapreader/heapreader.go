package main

import (
	"fmt"
	"os"
	"storemy/pkg/catalog/catalogmanager"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/debug/ui"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type heapKeyMap struct {
	ui.CommonKeyMap
	ui.NavigationKeyMap
}

var heapKeys = heapKeyMap{
	CommonKeyMap:     ui.CommonKeys,
	NavigationKeyMap: ui.NavigationKeys,
}

var (
	statsTable  = systemtable.Stats.TableName()
	colsTable   = systemtable.Columns.TableName()
	tablesTable = systemtable.Tables.TableName()
)

type tableInfo struct {
	tableID    primitives.FileID
	tableName  string
	filePath   string
	primaryKey string
	schema     *tuple.TupleDescription
}

type heapModel struct {
	catalog       *catalogmanager.CatalogManager
	store         *memory.PageStore
	currentView   string // "menu", "table_data", "page_view"
	cursor        int
	scrollOffset  int
	viewport      viewport.Model
	width         int
	height        int
	err           error
	dataDir       string
	tables        []tableInfo
	selectedTable *tableInfo
	currentPage   primitives.PageNumber
	totalPages    primitives.PageNumber
	tableData     [][]string
	columnHeaders []string
	rowCursor     int
}

func initialHeapModel(dataDir string) heapModel {
	return heapModel{
		dataDir:     dataDir,
		currentView: "loading",
		tables:      []tableInfo{},
	}
}

func (m heapModel) Init() tea.Cmd {
	return initializeHeapReader(m.dataDir)
}

type heapInitMsg struct {
	catalog *catalogmanager.CatalogManager
	store   *memory.PageStore
	tables  []tableInfo
	err     error
}

func initializeHeapReader(dataDir string) tea.Cmd {
	return func() tea.Msg {
		store := memory.NewPageStore(nil) // No WAL for reading
		cat := catalogmanager.NewCatalogManager(store, dataDir)

		tx := transaction.NewTransactionContext(primitives.NewTransactionID())
		if err := cat.Initialize(tx); err != nil {
			return heapInitMsg{err: err}
		}

		if err := cat.LoadAllTables(transaction.NewTransactionContext(primitives.NewTransactionID())); err != nil {
			return heapInitMsg{err: err}
		}

		tx2 := transaction.NewTransactionContext(primitives.NewTransactionID())

		// Collect all user tables (non-catalog tables
		var tables []tableInfo
		catalogTableID, _ := cat.GetTableID(tx2, tablesTable)
		columnsTableID, _ := cat.GetTableID(tx2, colsTable)
		statsTableID, _ := cat.GetTableID(tx2, statsTable)

		allTableNames, _ := cat.ListAllTables(tx2, true)
		for _, name := range allTableNames {
			tableID, err := cat.GetTableID(tx2, name)
			if err != nil {
				continue
			}

			// Skip catalog tables
			if tableID == catalogTableID || tableID == columnsTableID || tableID == statsTableID {
				continue
			}

			file, err := cat.GetTableFile(tableID)
			if err != nil {
				continue
			}

			schema := file.GetTupleDesc()

			tables = append(tables, tableInfo{
				tableID:    tableID,
				tableName:  name,
				filePath:   "", // We don't have direct access to file path here
				primaryKey: "", // We don't track primary key in TableManager
				schema:     schema,
			})
		}

		return heapInitMsg{
			catalog: cat,
			store:   store,
			tables:  tables,
		}
	}
}

type tableDataLoadedMsg struct {
	headers   []string
	data      [][]string
	pageCount primitives.PageNumber
	err       error
}

func loadTableData(cat *catalogmanager.CatalogManager, tableInfo *tableInfo) tea.Cmd {
	return func() tea.Msg {
		file, err := cat.GetTableFile(tableInfo.tableID)
		if err != nil {
			return tableDataLoadedMsg{err: err}
		}

		schema := file.GetTupleDesc()
		headers := make([]string, schema.NumFields())
		var i primitives.ColumnID
		for i = 0; i < schema.NumFields(); i++ {
			name, _ := schema.GetFieldName(i)
			headers[i] = name
		}

		hf, ok := file.(*heap.HeapFile)
		if !ok {
			return tableDataLoadedMsg{err: fmt.Errorf("not a heap file")}
		}

		iter := hf.Iterator(primitives.NewTransactionID())
		if err := iter.Open(); err != nil {
			return tableDataLoadedMsg{err: err}
		}
		defer iter.Close()

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
			var i primitives.ColumnID
			for i = 0; i < schema.NumFields(); i++ {
				field, err := tup.GetField(i)
				if err != nil {
					row[i] = "ERROR"
				} else {
					row[i] = formatField(field)
				}
			}
			data = append(data, row)
		}

		// Get page count
		var pageCount primitives.PageNumber = 0
		if hf, ok := file.(*heap.HeapFile); ok {
			pc, err := hf.NumPages()
			if err == nil {
				pageCount = pc
			}
		}

		return tableDataLoadedMsg{
			headers:   headers,
			data:      data,
			pageCount: pageCount,
		}
	}
}

type pageDataLoadedMsg struct {
	pageNo primitives.PageNumber
	tuples []*tuple.Tuple
	err    error
}

func loadPageData(cat *catalogmanager.CatalogManager, tableID primitives.FileID, pageNo primitives.PageNumber) tea.Cmd {
	return func() tea.Msg {
		file, err := cat.GetTableFile(tableID)
		if err != nil {
			return pageDataLoadedMsg{err: err}
		}

		hf, ok := file.(*heap.HeapFile)
		if !ok {
			return pageDataLoadedMsg{err: fmt.Errorf("not a heap file")}
		}

		pageID := page.NewPageDescriptor(tableID, pageNo)
		page, err := hf.ReadPage(pageID)
		if err != nil {
			return pageDataLoadedMsg{err: err}
		}

		heapPage, ok := page.(*heap.HeapPage)
		if !ok {
			return pageDataLoadedMsg{err: fmt.Errorf("not a heap page")}
		}

		var tuples []*tuple.Tuple
		iter := heap.NewHeapPageIterator(heapPage)
		if err := iter.Open(); err != nil {
			return pageDataLoadedMsg{err: err}
		}
		defer iter.Close()

		for {
			hasNext, err := iter.HasNext()
			if err != nil || !hasNext {
				break
			}

			tup, err := iter.Next()
			if err != nil || tup == nil {
				break
			}

			tuples = append(tuples, tup)
		}

		return pageDataLoadedMsg{
			pageNo: pageNo,
			tuples: tuples,
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
		return field.String()
	}
}

func (m heapModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case heapInitMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, tea.Quit
		}
		m.catalog = msg.catalog
		m.store = msg.store
		m.tables = msg.tables
		if len(m.tables) == 0 {
			m.currentView = "no_tables"
		} else {
			m.currentView = "menu"
		}
		return m, nil

	case tableDataLoadedMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.columnHeaders = msg.headers
		m.tableData = msg.data
		m.totalPages = msg.pageCount
		m.currentPage = 0
		m.rowCursor = 0
		m.currentView = "table_data"
		return m, nil

	case pageDataLoadedMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		// Convert tuples to display data
		m.currentView = "page_view"
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
			case key.Matches(msg, heapKeys.Quit):
				return m, tea.Quit
			case key.Matches(msg, heapKeys.Up):
				if m.cursor > 0 {
					m.cursor--
				}
			case key.Matches(msg, heapKeys.Down):
				if m.cursor < len(m.tables)-1 {
					m.cursor++
				}
			case key.Matches(msg, heapKeys.Select):
				if m.cursor < len(m.tables) {
					m.selectedTable = &m.tables[m.cursor]
					return m, loadTableData(m.catalog, m.selectedTable)
				}
			}

		case "table_data":
			switch {
			case key.Matches(msg, heapKeys.Quit):
				return m, tea.Quit
			case key.Matches(msg, heapKeys.Back):
				m.currentView = "menu"
				m.cursor = 0
				m.tableData = nil
				m.columnHeaders = nil
				m.selectedTable = nil
				return m, nil
			case key.Matches(msg, heapKeys.Up):
				if m.rowCursor > 0 {
					m.rowCursor--
				}
			case key.Matches(msg, heapKeys.Down):
				if m.rowCursor < len(m.tableData)-1 {
					m.rowCursor++
				}
			case key.Matches(msg, heapKeys.NextPage):
				if m.totalPages > 0 && m.currentPage < m.totalPages-1 {
					m.currentPage++
					return m, loadPageData(m.catalog, m.selectedTable.tableID, m.currentPage)
				}
			case key.Matches(msg, heapKeys.PrevPage):
				if m.currentPage > 0 {
					m.currentPage--
					return m, loadPageData(m.catalog, m.selectedTable.tableID, m.currentPage)
				}
			case key.Matches(msg, heapKeys.FirstPage):
				m.currentPage = 0
				return m, loadPageData(m.catalog, m.selectedTable.tableID, m.currentPage)
			case key.Matches(msg, heapKeys.LastPage):
				if m.totalPages > 0 {
					m.currentPage = m.totalPages - 1
					return m, loadPageData(m.catalog, m.selectedTable.tableID, m.currentPage)
				}
			case key.Matches(msg, heapKeys.Left):
				if m.scrollOffset > 0 {
					m.scrollOffset--
				}
			case key.Matches(msg, heapKeys.Right):
				m.scrollOffset++
			}

		case "page_view":
			switch {
			case key.Matches(msg, heapKeys.Quit):
				return m, tea.Quit
			case key.Matches(msg, heapKeys.Back):
				m.currentView = "table_data"
				return m, nil
			}
		}
	}

	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m heapModel) View() string {
	if m.err != nil {
		return ui.ErrorStyle.Render(fmt.Sprintf("Error: %v\n\nPress q to quit.", m.err))
	}

	var b strings.Builder

	title := ui.TitleStyle.Render("🗄  Heap Table Reader")
	b.WriteString(title + "\n\n")

	switch m.currentView {
	case "loading":
		b.WriteString("Loading tables...\n")
	case "no_tables":
		b.WriteString("No user tables found in the database.\n\n")
		b.WriteString(ui.HelpStyle.Render("Press q to quit"))
	case "menu":
		b.WriteString(m.renderMenu())
	case "table_data":
		b.WriteString(m.renderTableData())
	case "page_view":
		b.WriteString(m.renderPageView())
	}

	statusBar := m.renderStatusBar()
	b.WriteString("\n" + statusBar)

	return b.String()
}

func (m heapModel) renderMenu() string {
	var b strings.Builder

	header := ui.HeaderStyle.Render(fmt.Sprintf(" User Tables (%d) ", len(m.tables)))
	b.WriteString(header + "\n\n")

	for i, table := range m.tables {
		tableInfo := fmt.Sprintf("%s (ID: %d, Fields: %d)",
			table.tableName,
			table.tableID,
			table.schema.NumFields())

		if table.primaryKey != "" {
			tableInfo += fmt.Sprintf(" [PK: %s]", table.primaryKey)
		}

		if i == m.cursor {
			b.WriteString(ui.SelectedItemStyle.Render("▶ "+tableInfo) + "\n")
		} else {
			b.WriteString(ui.ItemStyle.Render("  "+tableInfo) + "\n")
		}
	}

	b.WriteString("\n")
	b.WriteString(ui.HelpStyle.Render("↑/↓: navigate | enter: view table | q: quit"))

	return b.String()
}

func (m heapModel) renderTableData() string {
	if len(m.tableData) == 0 {
		return "No data in this table.\n\n" + ui.HelpStyle.Render("Press esc to go back | q to quit")
	}

	var b strings.Builder

	// Table info
	tableTitle := ui.HeaderStyle.Render(fmt.Sprintf(" %s (%d rows, %d pages) ",
		m.selectedTable.tableName,
		len(m.tableData),
		m.totalPages))
	b.WriteString(tableTitle + "\n\n")

	// Calculate column widths
	colWidths := make([]int, len(m.columnHeaders))
	for i, header := range m.columnHeaders {
		colWidths[i] = len(header)
	}
	for _, row := range m.tableData {
		for i, cell := range row {
			if i < len(colWidths) && len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	// Apply max width and account for scroll
	maxColWidth := 30
	visibleCols := make([]int, 0)
	for i := range m.columnHeaders {
		if i >= m.scrollOffset && len(visibleCols) < 10 { // Show max 10 columns at once
			if colWidths[i] > maxColWidth {
				colWidths[i] = maxColWidth
			}
			visibleCols = append(visibleCols, i)
		}
	}

	// Render headers
	headerRow := ""
	for _, colIdx := range visibleCols {
		headerRow += ui.TableHeaderStyle.Render(padString(m.columnHeaders[colIdx], colWidths[colIdx]))
		headerRow += " "
	}
	b.WriteString(headerRow + "\n")

	// Render separator
	separator := ""
	for _, colIdx := range visibleCols {
		separator += strings.Repeat("─", colWidths[colIdx]+2)
		separator += "┼"
	}
	b.WriteString(lipgloss.NewStyle().Foreground(ui.MutedColor).Render(separator) + "\n")

	// Render data rows
	visibleStart := max(0, m.rowCursor-10)
	visibleEnd := min(len(m.tableData), visibleStart+20)

	for i := visibleStart; i < visibleEnd; i++ {
		row := m.tableData[i]
		rowStr := ""
		for _, colIdx := range visibleCols {
			if colIdx < len(row) {
				cellContent := row[colIdx]
				if len(cellContent) > maxColWidth {
					cellContent = cellContent[:maxColWidth-3] + "..."
				}
				cellContent = padString(cellContent, colWidths[colIdx])
				if i == m.rowCursor {
					rowStr += ui.SelectedItemStyle.Render(cellContent)
				} else {
					rowStr += ui.CellStyle.Render(cellContent)
				}
				rowStr += " "
			}
		}
		b.WriteString(rowStr + "\n")
	}

	b.WriteString("\n")
	b.WriteString(ui.HelpStyle.Render("↑/↓: navigate rows | ←/→: scroll columns | n/p: next/prev page | esc: back | q: quit"))

	return b.String()
}

func (m heapModel) renderPageView() string {
	return "Page view not yet implemented\n\n" + ui.HelpStyle.Render("Press esc to go back")
}

func (m heapModel) renderStatusBar() string {
	var status string
	switch m.currentView {
	case "menu":
		status = fmt.Sprintf(" Menu | Data Directory: %s | Tables: %d ", m.dataDir, len(m.tables))
	case "table_data":
		if m.selectedTable != nil {
			position := fmt.Sprintf("%d/%d", m.rowCursor+1, len(m.tableData))
			pageInfo := fmt.Sprintf("Page %d/%d", m.currentPage+1, m.totalPages)
			status = fmt.Sprintf(" %s | Row: %s | %s ", m.selectedTable.tableName, position, pageInfo)
		}
	case "page_view":
		status = fmt.Sprintf(" Page View | Page %d/%d ", m.currentPage+1, m.totalPages)
	default:
		status = " Loading... "
	}
	return ui.StatusBarStyle.Render(status)
}

func padString(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: heapreader <data-directory>")
		os.Exit(1)
	}

	dataDir := os.Args[1]

	p := tea.NewProgram(
		initialHeapModel(dataDir),
		tea.WithAltScreen(),
	)

	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
