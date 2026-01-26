# PolySpy 🐋

**The Audited Intelligence Platform for Polymarket**

PolySpy is a premium SaaS dashboard that tracks "Smart Money" on Polymarket using accounting-style credit ratings and behavioral analysis.

## 🎯 Core Features

### 1. Whale Credit Rating Engine ("Audited Whale" Strategy)
- Calculates a "Whale Credit Score" (0-100) for every wallet
- Assigns grades: AAA (God Whale), AA, A, B, C, F (Whale Trap)
- **Metrics Used:**
  - Win Rate: % of profitable trades
  - PnL Ratio: Average Win / Average Loss
  - Consistency: Standard deviation of returns
  - Volume-adjusted confidence bonus

### 2. Persona Auto-Tagging ("Narrative" Strategy)
- Auto-assigns memorable personas based on behavior patterns:
  - **The Insider**: High volume buys before market moves
  - **The Contrarian**: Bets against the crowd and wins
  - **The Whale Trap**: High volume but terrible win rate (COUNTER-SIGNAL)
  - Diamond Hands, The Degen, The Sniper, The Accumulator

### 3. Fire Exit Detection ("Exit Liquidity Savior" Strategy)
- Detects when smart money dumps positions
- Alerts when AAA/A grade whales sell >50% of position
- Separate visual treatment for DUMP_ALERT signals

### 4. Contextual EV Cards ("Copy-Trading" Strategy)
- Displays "Copy EV" calculation: `((1 - Price) / Price) * 100%`
- Direct trade links to Polymarket
- Bloomberg Terminal-inspired dark mode UI

## 🏗️ Tech Stack

### Backend
- **Python** with FastAPI
- **Supabase** (PostgreSQL)
- Modular services architecture

### Frontend
- **Next.js 14** (App Router)
- **Tailwind CSS** + custom terminal theme
- **Shadcn UI** components (dark mode)

## 📁 Project Structure

```
polymarket/
├── database/
│   └── schema.sql           # Supabase schema with all tables
├── backend/
│   ├── models/
│   │   └── schemas.py       # Pydantic data models
│   ├── services/
│   │   ├── credit_rating.py # CreditRatingService
│   │   ├── persona_engine.py # PersonaEngine
│   │   └── exit_detector.py  # ExitDetector
│   ├── worker.py            # Main processing pipeline
│   └── requirements.txt
└── frontend/
    ├── app/
    │   ├── layout.tsx
    │   ├── page.tsx         # Dashboard
    │   └── globals.css
    ├── components/
    │   └── dashboard/
    │       └── signal-card.tsx  # Main signal card component
    ├── lib/
    │   └── utils.ts
    └── types/
        └── index.ts
```

## 🚀 Getting Started

### Backend
```bash
cd backend
pip install -r requirements.txt
python worker.py  # Run with mock data
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```

## 📊 Database Schema

Key tables:
- `wallets` - Whale profiles with credit grades and personas
- `transactions` - All trades with dump alert flags
- `signals` - Generated signals for the dashboard
- `markets` - Polymarket market metadata

## 🎨 UI Design

Bloomberg Terminal inspired:
- Dark mode (#0a0a0a background)
- Neon accents (green for profit, red for dumps)
- Monospace fonts for numbers
- Grid background effect

## 📈 Credit Rating Logic

```python
# Quick Grade Logic (from PRD)
if win_rate > 70 and pnl_ratio > 2.0:
    return "AAA"  # God Whale
elif win_rate > 50 and pnl_ratio > 1.5:
    return "A"    # Smart Money
else:
    return "B" or "C"  # Gambler
```

## ⚠️ Disclaimer

This is for educational and informational purposes only. Not financial advice. Always do your own research before trading.

---

Built with 🐋 by PolySpy Team
