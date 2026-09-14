import Link from "next/link";

const sourceUrl = "https://www.usaspending.gov/";

export function LandingPage() {
  return (
    <main className="landing-shell">
      <header className="landing-nav">
        <Link className="landing-brand" href="/" aria-label="Govtracts home">
          <span className="brand-mark" aria-hidden="true">⌁</span>
          GOVTRACTS
        </Link>
        <nav aria-label="Primary navigation">
          <Link href="/market">Market dashboard</Link>
          <a href={sourceUrl} target="_blank" rel="noreferrer">Data source</a>
        </nav>
        <Link className="nav-action" href="/market">Open dashboard <span aria-hidden="true">↗</span></Link>
      </header>

      <section className="landing-hero" aria-labelledby="landing-title">
        <div className="landing-intro">
          <p className="eyebrow">Federal contract research</p>
          <h1 id="landing-title">Find the signal in federal Cyber/IT awards.</h1>
          <p className="landing-copy">Govtracts turns source-attributed USAspending award data into a focused view of agency activity, recipient momentum, and contract-market context.</p>
          <div className="landing-actions">
            <Link className="primary-action" href="/market">Explore the market <span aria-hidden="true">→</span></Link>
            <a className="quiet-action" href={sourceUrl} target="_blank" rel="noreferrer">About the public data <span aria-hidden="true">↗</span></a>
          </div>
          <div className="preset-strip">
            <span className="signal-dot" aria-hidden="true" />
            <p><strong>Cyber/IT preset</strong><span>Selected NAICS, PSC, and keyword rules are visible in the research workspace.</span></p>
          </div>
        </div>

        <aside className="research-brief" aria-label="Govtracts research capabilities">
          <div className="brief-heading"><p>Research workspace</p><span className="brief-status"><i aria-hidden="true" /> Source-attributed</span></div>
          <div className="brief-item"><span className="brief-number">01</span><div><h2>Award market dashboard</h2><p>Review total obligations and spending movement over time.</p></div></div>
          <div className="brief-item"><span className="brief-number">02</span><div><h2>Agency &amp; recipient leaders</h2><p>See which organizations are awarding and receiving Cyber/IT work.</p></div></div>
          <div className="brief-item"><span className="brief-number">03</span><div><h2>Recent award context</h2><p>Open award-level records with their original public source links.</p></div></div>
          <Link className="brief-link" href="/market">Go to the market dashboard <span aria-hidden="true">→</span></Link>
        </aside>
      </section>

      <section className="landing-value" aria-label="How Govtracts supports market research">
        <p className="section-label">Built for early market research</p>
        <div className="value-grid">
          <article><span>01</span><h2>Start with a defined lens</h2><p>The Cyber/IT preset is a transparent market-research filter, not a claim to capture all cybersecurity spending.</p></article>
          <article><span>02</span><h2>Follow the money</h2><p>Compare obligation totals, award volume, agencies, and recipients from the same working surface.</p></article>
          <article><span>03</span><h2>Verify the record</h2><p>Every displayed award remains traceable to the official USAspending source.</p></article>
        </div>
      </section>

      <footer className="landing-footer">
        <span>Govtracts · Federal Cyber/IT market intelligence</span>
        <a href={sourceUrl} target="_blank" rel="noreferrer">Data: USAspending.gov <span aria-hidden="true">↗</span></a>
      </footer>
    </main>
  );
}
