import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Govtracts | Federal contract intelligence",
  description: "Source-attributed federal Cyber/IT contract market research.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
