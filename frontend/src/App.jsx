import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import { useState } from 'react'
import RouteList from './components/RouteList'
import RouteDetail from './components/RouteDetail'
import './App.css'

function App() {
  const [refreshing, setRefreshing] = useState(false)
  const [lastUpdated, setLastUpdated] = useState(null)

  const handleRefresh = () => {
    setRefreshing(true)
    // Trigger refresh - this will be handled by child components
    window.location.reload()
  }

  return (
    <Router>
      <div className="app">
        <header>
          <div className="header-content">
            <div>
              <h1>WMATA Performance Dashboard</h1>
              <p className="subtitle">Real-time transit performance metrics</p>
            </div>
            <div className="header-actions">
              <button
                onClick={handleRefresh}
                disabled={refreshing}
                className="refresh-btn"
                title="Refresh data"
              >
                <span className={refreshing ? 'refresh-icon spinning' : 'refresh-icon'}>↻</span>
                {refreshing ? 'Refreshing...' : 'Refresh'}
              </button>
            </div>
          </div>
        </header>

        <Routes>
          <Route path="/" element={<RouteList />} />
          <Route path="/route/:routeId" element={<RouteDetail />} />
        </Routes>
      </div>
    </Router>
  )
}

export default App
