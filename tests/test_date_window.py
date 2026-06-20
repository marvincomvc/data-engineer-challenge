"""
Testes unitários da lógica de janela de 7 dias.

Cobre a função dentro_da_janela() do módulo src.transform.
"""

import pytest
from src.transform import dentro_da_janela


class TestJanelaDatas:
    """Testes da função dentro_da_janela (janela de 7 dias)."""

    REFERENCIA = "2025-03-15"

    def test_data_exatamente_na_referencia(self):
        """created_at exatamente na reference_date deve estar dentro da janela."""
        assert dentro_da_janela("2025-03-15T14:00:00Z", self.REFERENCIA) is True

    def test_data_no_inicio_da_referencia(self):
        """created_at em 00:00:00 da reference_date deve estar na janela."""
        assert dentro_da_janela("2025-03-15T00:00:00Z", self.REFERENCIA) is True

    def test_data_no_fim_da_referencia(self):
        """created_at em 23:59:59 da reference_date deve estar na janela."""
        assert dentro_da_janela("2025-03-15T23:59:59Z", self.REFERENCIA) is True

    def test_data_7_dias_antes(self):
        """created_at exatamente 7 dias antes (2025-03-08) deve estar na janela."""
        assert dentro_da_janela("2025-03-08T00:00:00Z", self.REFERENCIA) is True
        assert dentro_da_janela("2025-03-08T23:59:59Z", self.REFERENCIA) is True

    def test_data_6_dias_antes(self):
        """created_at 6 dias antes (2025-03-09) deve estar na janela."""
        assert dentro_da_janela("2025-03-09T12:00:00Z", self.REFERENCIA) is True

    def test_data_3_dias_antes(self):
        """created_at 3 dias antes (2025-03-12) deve estar na janela."""
        assert dentro_da_janela("2025-03-12T10:30:00Z", self.REFERENCIA) is True

    def test_data_8_dias_antes_fora_da_janela(self):
        """created_at 8 dias antes (2025-03-07) deve estar fora da janela."""
        assert dentro_da_janela("2025-03-07T23:59:59Z", self.REFERENCIA) is False

    def test_data_1_dia_depois_fora_da_janela(self):
        """created_at 1 dia depois (2025-03-16) deve estar fora da janela."""
        assert dentro_da_janela("2025-03-16T00:00:00Z", self.REFERENCIA) is False

    def test_data_muito_antes_fora_da_janela(self):
        """created_at 30 dias antes deve estar fora da janela."""
        assert dentro_da_janela("2025-02-13T12:00:00Z", self.REFERENCIA) is False

    def test_data_muito_depois_fora_da_janela(self):
        """created_at 30 dias depois deve estar fora da janela."""
        assert dentro_da_janela("2025-04-14T12:00:00Z", self.REFERENCIA) is False

    def test_data_sem_timezone_z(self):
        """created_at sem sufixo Z ainda deve ser interpretado corretamente."""
        # Data com +00:00 explicito
        assert dentro_da_janela("2025-03-15T14:00:00+00:00", self.REFERENCIA) is True

    def test_data_com_utc_explicito(self):
        """created_at com offset +00:00."""
        assert dentro_da_janela("2025-03-10T10:00:00+00:00", self.REFERENCIA) is True

    def test_outra_reference_date(self):
        """Testa com uma reference_date diferente."""
        nova_ref = "2025-04-01"
        assert dentro_da_janela("2025-04-01T12:00:00Z", nova_ref) is True
        assert dentro_da_janela("2025-03-25T00:00:00Z", nova_ref) is True  # 7 dias antes
        assert dentro_da_janela("2025-03-24T23:59:59Z", nova_ref) is False  # 8 dias antes