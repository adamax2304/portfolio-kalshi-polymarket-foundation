import Link from "next/link";

const links = [
  { href: "/", label: "System Overview" },
  { href: "/streams", label: "Streams" },
  { href: "/markets", label: "Markets" },
  { href: "/data", label: "Data" },
  { href: "/logs", label: "Logs / Alerts" },
  { href: "/fair-value", label: "Fair Value Monitor" },
  { href: "/signals", label: "Signals" },
  { href: "/replay-runs", label: "Replay Runs" },
  { href: "/paper-positions", label: "Paper Positions" },
  { href: "/analytics", label: "Analytics" }
];

export function Nav() {
  return (
    <nav className="nav">
      {links.map((item) => (
        <Link key={item.href} href={item.href}>
          {item.label}
        </Link>
      ))}
    </nav>
  );
}
