export interface Alert {
  id: string;
  timestamp: number;
  marketTitle: string;
  assetId: string;
  side: string;
  price: number;
  size: number;
  usdAmount: number;
  txHash: string;
  wallet: string;
  walletAgeDays: number | null;
  absorptionPct: number | null;
  score: number | null;
  hoursToResolution: number | null;
  status: 'pending' | 'passed' | 'skipped';
}
