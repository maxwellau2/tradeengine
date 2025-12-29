use std::io::{self, Stdout};
use std::time::Duration;

use arc_swap::ArcSwap;
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap},
};
use std::sync::Arc;

use super::state::{SharedTUIState, request_shutdown};

type Term = Terminal<CrosstermBackend<Stdout>>;

pub struct TUI {
    terminal: Term,
    state: SharedTUIState,
}

impl TUI {
    pub fn new(state: SharedTUIState) -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;
        Ok(Self { terminal, state })
    }

    pub fn run(&mut self) -> io::Result<()> {
        loop {
            self.draw()?;

            // poll for input with 100ms timeout (10fps refresh)
            if event::poll(Duration::from_millis(100))? {
                if let Event::Key(key) = event::read()? {
                    if key.code == KeyCode::Char('q') {
                        // signal all threads to shutdown
                        request_shutdown();
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn draw(&mut self) -> io::Result<()> {
        // load() is lock-free - never blocks, always returns latest state
        let state = self.state.load();
        let state = (*state).clone();

        self.terminal.draw(|f| {
            // split into left (data) and right (logs)
            let main_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .margin(1)
                .constraints([
                    Constraint::Percentage(50), // data panels
                    Constraint::Percentage(50), // logs
                ])
                .split(f.area());

            // left side: orders, positions, balances
            let left_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(45), // orders
                    Constraint::Percentage(35), // positions
                    Constraint::Percentage(20), // balances
                ])
                .split(main_chunks[0]);

            // orders table
            let order_rows: Vec<Row> = state
                .orders
                .iter()
                .map(|o| {
                    Row::new(vec![
                        Cell::from(o.client_order_id.to_string()),
                        Cell::from(o.symbol.to_string()),
                        Cell::from(format!("{:?}", o.side)),
                        Cell::from(format!("{:.4}", o.price)),
                        Cell::from(format!("{:.4}", o.qty)),
                        Cell::from(format!("{:?}", o.state)),
                    ])
                })
                .collect();

            let orders_table = Table::new(
                order_rows,
                [
                    Constraint::Length(20),
                    Constraint::Length(8),
                    Constraint::Length(5),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(12),
                ],
            )
            .header(
                Row::new(vec!["ClOID", "Symbol", "Side", "Price", "Qty", "State"]).style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            )
            .block(
                Block::default()
                    .title(Span::styled(
                        format!(" Orders ({}) ", state.orders.len()),
                        Style::default().fg(Color::Cyan),
                    ))
                    .borders(Borders::ALL),
            );

            f.render_widget(orders_table, left_chunks[0]);

            // positions table
            let position_rows: Vec<Row> = state
                .positions
                .iter()
                .map(|p| {
                    let pnl_color = if p.unrealised_pnl >= 0.0 {
                        Color::Green
                    } else {
                        Color::Red
                    };
                    Row::new(vec![
                        Cell::from(p.symbol.to_string()),
                        Cell::from(format!("{:?}", p.side)),
                        Cell::from(format!("{:.4}", p.qty)),
                        Cell::from(format!("{:+.2}", p.unrealised_pnl))
                            .style(Style::default().fg(pnl_color)),
                    ])
                })
                .collect();

            let positions_table = Table::new(
                position_rows,
                [
                    Constraint::Length(10),
                    Constraint::Length(6),
                    Constraint::Length(12),
                    Constraint::Length(12),
                ],
            )
            .header(
                Row::new(vec!["Symbol", "Side", "Qty", "PnL"]).style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            )
            .block(
                Block::default()
                    .title(Span::styled(
                        format!(" Positions ({}) ", state.positions.len()),
                        Style::default().fg(Color::Cyan),
                    ))
                    .borders(Borders::ALL),
            );

            f.render_widget(positions_table, left_chunks[1]);

            // balances table
            let balance_rows: Vec<Row> = state
                .balances
                .iter()
                .map(|b| {
                    Row::new(vec![
                        Cell::from(b.coin.to_string()),
                        Cell::from(format!("{:.4}", b.qty)),
                    ])
                })
                .collect();

            let balances_table = Table::new(
                balance_rows,
                [Constraint::Length(10), Constraint::Length(15)],
            )
            .header(
                Row::new(vec!["Coin", "Balance"]).style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            )
            .block(
                Block::default()
                    .title(Span::styled(
                        format!(" Balances ({}) ", state.balances.len()),
                        Style::default().fg(Color::Cyan),
                    ))
                    .borders(Borders::ALL),
            );

            f.render_widget(balances_table, left_chunks[2]);

            // right side: logs with word wrapping
            let log_lines: Vec<Line> = state
                .logs
                .iter()
                .rev() // newest first
                .take(50)
                .map(|log| {
                    let color = if log.contains("ERROR") {
                        Color::Red
                    } else if log.contains("WARN") {
                        Color::Yellow
                    } else if log.contains("DEBUG") {
                        Color::DarkGray
                    } else {
                        Color::White
                    };
                    Line::from(Span::styled(log.as_str(), Style::default().fg(color)))
                })
                .collect();

            let logs_paragraph = Paragraph::new(log_lines)
                .block(
                    Block::default()
                        .title(Span::styled(" Logs ", Style::default().fg(Color::Cyan)))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false });

            f.render_widget(logs_paragraph, main_chunks[1]);
        })?;

        Ok(())
    }
}

impl Drop for TUI {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
    }
}
