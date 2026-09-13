export type Award = {
  id: string;
  award_id: string;
  agency: string | null;
  vendor: string | null;
  obligation_amount: number | null;
  base_obligation_date: string | null;
  source_url: string | null;
};

export type Overview = {
  preset: { label: string; description: string; disclaimer: string };
  total_obligations: number;
  award_count: number;
  trend: { period: string; amount: number }[];
  top_agencies: { name: string; code: string; amount: number; award_count: number }[];
  top_vendors: { name: string; amount: number; award_count: number }[];
  recent_awards: Award[];
  source: { name: string; url: string; last_successful_refresh: string | null };
};

export async function getOverview(signal?: AbortSignal): Promise<Overview> {
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:8000";
  const response = await fetch(`${baseUrl}/v1/market-overview`, { signal });
  if (!response.ok) throw new Error("The Govtracts API could not load market data.");
  return response.json() as Promise<Overview>;
}

export const money = (value: number | null) => new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(value ?? 0);
