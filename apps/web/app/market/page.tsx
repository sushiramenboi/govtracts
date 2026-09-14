import { MarketDashboard } from "../../components/market-dashboard";
import { getOverview } from "../../lib/api";

export const dynamic = "force-dynamic";

export default async function MarketPage() {
  const overview = await loadOverview();

  return (
    <MarketDashboard
      initialOverview={overview}
      initialError={
        overview
          ? null
          : "Unable to load live market data. Ensure the Govtracts API is running."
      }
    />
  );
}

async function loadOverview() {
  try {
    return await getOverview();
  } catch {
    return null;
  }
}
