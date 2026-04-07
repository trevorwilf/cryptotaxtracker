"""
Tests for the auto-recompute feature.

After every successful sync_all, the system should automatically trigger
a v4 recompute for the current tax year (when TAX_AUTO_RECOMPUTE=true).
"""
import inspect
import pytest
from datetime import datetime, timezone
from unittest.mock import patch, AsyncMock, MagicMock


class TestAutoRecompute:

    def test_config_has_auto_recompute_setting(self):
        from config import Settings
        with patch.dict("os.environ", {"TAX_AUTO_RECOMPUTE": "true"}):
            s = Settings()
            assert s.auto_recompute is True
        with patch.dict("os.environ", {"TAX_AUTO_RECOMPUTE": "false"}):
            s = Settings()
            assert s.auto_recompute is False
        # Default is true
        with patch.dict("os.environ", {}, clear=False):
            s = Settings()
            assert s.auto_recompute is True

    def test_compute_v4_full_extracted(self):
        """_compute_v4_full should be a standalone async function."""
        import main as m
        assert hasattr(m, '_compute_v4_full')
        assert inspect.iscoroutinefunction(m._compute_v4_full)

    def test_run_recompute_helper_exists(self):
        import main as m
        assert hasattr(m, '_run_recompute_after_sync')
        assert inspect.iscoroutinefunction(m._run_recompute_after_sync)

    def test_run_sync_all_calls_recompute(self):
        """run_sync_all should call _run_recompute_after_sync."""
        import main as m
        source = inspect.getsource(m.run_sync_all)
        assert "_run_recompute_after_sync" in source

    def test_single_sync_also_triggers_recompute(self):
        """run_sync (single exchange) should also recompute when recompute=True."""
        import main as m
        source = inspect.getsource(m.run_sync)
        assert "_run_recompute_after_sync" in source

    def test_recompute_status_dict_exists(self):
        import main as m
        assert hasattr(m, 'recompute_status')
        assert isinstance(m.recompute_status, dict)

    def test_sync_status_includes_recompute(self):
        """GET /sync/status should return recompute info."""
        import main as m
        source = inspect.getsource(m.get_sync_status)
        assert "auto_recompute_enabled" in source
        assert "last_recompute" in source
        assert "recompute_status" in source

    def test_compute_all_defaults_to_current_year(self):
        """v4_compute_all should default year to current year, not 2025."""
        import main as m
        source = inspect.getsource(m.v4_compute_all)
        assert "Query(2025)" not in source
        assert "Query(None)" in source
        assert "datetime.now" in source

    def test_recompute_helper_checks_setting(self):
        """_run_recompute_after_sync should respect auto_recompute setting."""
        import main as m
        source = inspect.getsource(m._run_recompute_after_sync)
        assert "settings.auto_recompute" in source

    def test_recompute_helper_uses_current_year(self):
        import main as m
        source = inspect.getsource(m._run_recompute_after_sync)
        assert "datetime.now(timezone.utc).year" in source

    def test_recompute_helper_catches_exceptions(self):
        """Recompute failures should not propagate."""
        import main as m
        source = inspect.getsource(m._run_recompute_after_sync)
        assert "except Exception" in source

    @pytest.mark.asyncio
    async def test_recompute_disabled_does_nothing(self):
        """When auto_recompute=False, no compute should happen."""
        import main as m
        old_val = m.settings.auto_recompute
        m.settings.auto_recompute = False
        try:
            with patch.object(m, '_compute_v4_full', new_callable=AsyncMock) as mock_compute:
                await m._run_recompute_after_sync()
                mock_compute.assert_not_called()
        finally:
            m.settings.auto_recompute = old_val

    @pytest.mark.asyncio
    async def test_recompute_enabled_calls_compute(self):
        """When auto_recompute=True, compute should be called."""
        import main as m
        old_val = m.settings.auto_recompute
        m.settings.auto_recompute = True
        try:
            mock_result = {"run_id": 99, "compute": {"net_total": "1.23"}}
            with patch.object(m, '_compute_v4_full', new_callable=AsyncMock, return_value=mock_result):
                await m._run_recompute_after_sync()
                assert m.recompute_status.get("status") == "success"
                assert m.recompute_status.get("run_id") == 99
        finally:
            m.settings.auto_recompute = old_val

    @pytest.mark.asyncio
    async def test_recompute_failure_logged_not_raised(self):
        """Recompute failure should update status but not raise."""
        import main as m
        old_val = m.settings.auto_recompute
        m.settings.auto_recompute = True
        try:
            with patch.object(m, '_compute_v4_full', new_callable=AsyncMock,
                              side_effect=RuntimeError("DB down")):
                await m._run_recompute_after_sync()  # Should not raise
                assert m.recompute_status.get("status") == "error"
                assert "DB down" in m.recompute_status.get("error", "")
        finally:
            m.settings.auto_recompute = old_val
