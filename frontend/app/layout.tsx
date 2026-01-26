import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'PolySpy | Whale Intelligence Platform',
  description: 'The only audited intelligence platform for Polymarket. Track smart money, detect exits, and copy winning trades.',
  keywords: ['Polymarket', 'Whale Tracking', 'Prediction Markets', 'Crypto Trading', 'Smart Money'],
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-screen bg-terminal-bg antialiased">
        {/* Background grid effect */}
        <div className="fixed inset-0 terminal-grid opacity-20 pointer-events-none" />

        {/* Main content */}
        <div className="relative z-10">
          {children}
        </div>
      </body>
    </html>
  );
}
