"use client";

import Link from "next/link";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { money, type Overview } from "../lib/api";

type MarketDashboardProps = {
  initialOverview: Overview | null;
  initialError?: string | null;
};

export function MarketDashboard({ initialOverview, initialError = null }: MarketDashboardProps) {
  const overview = initialOverview;
  const error = initialError;

  if (error) return <main className="dashboard-shell"><DashboardHeader /><section className="notice error">{error}</section></main>;
  if (!overview) return <main className="dashboard-shell"><DashboardHeader /><section className="notice">Loading USAspending market data…</section></main>;
  if (overview.award_count === 0) return <main className="dashboard-shell"><DashboardHeader /><Preset overview={overview} /><section className="notice">No awards are available yet. Run the USAspending ingestion command, then refresh this page.</section></main>;

  return <main className="dashboard-shell">
    <DashboardHeader />
    <Preset overview={overview} />
    <section className="metrics">
      <article><span>Total obligations</span><strong>{money(overview.total_obligations)}</strong></article>
      <article><span>Awards tracked</span><strong>{overview.award_count.toLocaleString()}</strong></article>
      <article><span>Last updated</span><strong>{overview.source.last_successful_refresh ? new Date(overview.source.last_successful_refresh).toLocaleString() : "Not yet refreshed"}</strong></article>
    </section>
    <section className="panel chart"><h2>Spending over time</h2><ResponsiveContainer width="100%" height={280}><AreaChart data={overview.trend}><defs><linearGradient id="obligations" x1="0" x2="0" y1="0" y2="1"><stop offset="5%" stopColor="#35d0b1" stopOpacity={0.5}/><stop offset="95%" stopColor="#35d0b1" stopOpacity={0}/></linearGradient></defs><CartesianGrid stroke="#203746" strokeDasharray="3 3"/><XAxis dataKey="period" tick={{fill:"#a9bdc9"}}/><YAxis tickFormatter={(value) => `$${Number(value / 1_000_000).toFixed(0)}M`} tick={{fill:"#a9bdc9"}}/><Tooltip formatter={(value) => money(Number(value))}/><Area type="monotone" dataKey="amount" stroke="#35d0b1" fill="url(#obligations)" /></AreaChart></ResponsiveContainer></section>
    <section className="rankings">
      <Ranked title="Top awarding agencies" rows={overview.top_agencies.map((row) => [row.name, row.code, money(row.amount)])}/>
      <Ranked title="Top recipients" rows={overview.top_vendors.map((row) => [row.name, "", money(row.amount)])}/>
    </section>
    <section className="panel"><h2>Recent awards</h2><div className="table-wrap"><table><thead><tr><th>Award</th><th>Recipient</th><th>Agency</th><th>Date</th><th>Amount</th></tr></thead><tbody>{overview.recent_awards.map((award) => <tr key={award.id}><td>{award.source_url ? <a href={award.source_url} target="_blank" rel="noreferrer">{award.award_id}</a> : award.award_id}</td><td>{award.vendor ?? "Unspecified"}</td><td>{award.agency ?? "Unspecified"}</td><td>{award.base_obligation_date ?? "—"}</td><td>{money(award.obligation_amount)}</td></tr>)}</tbody></table></div></section>
    <p className="attribution">Data source: <a href={overview.source.url} target="_blank" rel="noreferrer">{overview.source.name}</a>. Award amounts are displayed for the selected Cyber/IT market-research preset.</p>
  </main>;
}

function DashboardHeader() { return <header className="dashboard-header"><Link className="brand" href="/">Govtracts</Link><span>Federal Cyber/IT market intelligence</span></header>; }
function Preset({ overview }: { overview: Overview }) { return <section className="preset"><p className="eyebrow">{overview.preset.label}</p><h1>Federal Cyber/IT contract market</h1><p>{overview.preset.description} {overview.preset.disclaimer}</p></section>; }
function Ranked({ title, rows }: { title: string; rows: string[][] }) { return <section className="panel"><h2>{title}</h2><ol className="rank-list">{rows.map(([name, code, amount]) => <li key={`${name}-${code}`}><span><b>{name}</b>{code && <small>{code}</small>}</span><strong>{amount}</strong></li>)}</ol></section>; }
