import type { Metadata } from "next";
import { Nav } from "@/components/nav";
import "./globals.css";

export const metadata: Metadata = {
  title: "Phase 1 Ops Dashboard",
  description: "Kalshi + Polymarket foundation operational dashboard"
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <div className="shell">
          <header className="top">
            <div>
              <div className="title">Phase 1 Foundation Dashboard</div>
              <div className="sub">Health, stream state, normalized contracts, recorder writes</div>
            </div>
            <Nav />
          </header>
          {children}
        </div>
      </body>
    </html>
  );
}

