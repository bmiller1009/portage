import { NavLink, Route, Routes } from "react-router-dom";
import RunsPage from "./pages/Runs";
import RunDetailPage from "./pages/RunDetail";
import WorkloadsPage from "./pages/Workloads";
import EnvironmentsPage from "./pages/Environments";
import DatasetsPage from "./pages/Datasets";
import ProvidersPage from "./pages/Providers";
import ConformancePage from "./pages/Conformance";
import SystemPage from "./pages/System";

const NAV_ITEMS = [
  { to: "/runs", label: "Runs" },
  { to: "/workloads", label: "Workloads" },
  { to: "/environments", label: "Environments" },
  { to: "/datasets", label: "Datasets" },
  { to: "/providers", label: "Providers" },
  { to: "/conformance", label: "Conformance" },
  { to: "/system", label: "System" },
];

export default function App() {
  return (
    <div className="app">
      <header className="app-header">
        <span className="app-title">Portage</span>
        <nav className="app-nav">
          {NAV_ITEMS.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) => (isActive ? "nav-link active" : "nav-link")}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
      </header>
      <main className="app-main">
        <Routes>
          <Route path="/" element={<RunsPage />} />
          <Route path="/runs" element={<RunsPage />} />
          <Route path="/runs/:runId" element={<RunDetailPage />} />
          <Route path="/workloads" element={<WorkloadsPage />} />
          <Route path="/environments" element={<EnvironmentsPage />} />
          <Route path="/datasets" element={<DatasetsPage />} />
          <Route path="/providers" element={<ProvidersPage />} />
          <Route path="/conformance" element={<ConformancePage />} />
          <Route path="/system" element={<SystemPage />} />
        </Routes>
      </main>
    </div>
  );
}
