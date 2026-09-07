import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
// App.css (tokens + shared primitives) must be emitted BEFORE every
// component-scoped stylesheet so a component rule can override a shared
// one at equal specificity. Importing it here, ahead of App.jsx, fixes
// the bundle order — App.jsx's own import would land after the component
// modules it imports (NOTES-85 review).
import './App.css'
import App from './App.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
