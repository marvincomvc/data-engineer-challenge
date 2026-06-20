"""
Testes unitários da lógica de tolerância de R$ 0,01.

Cobre a função aplicar_tolerancia() do módulo src.transform.
"""

import pytest
from src.transform import aplicar_tolerancia


class TestTolerancia:
    """Testes da função aplicar_tolerancia."""

    def test_diferenca_zero_esta_na_tolerancia(self):
        """Diferença de 0.00 deve estar dentro da tolerância."""
        assert aplicar_tolerancia(150.00, 150.00) is True

    def test_diferenca_de_um_centavo_esta_na_tolerancia(self):
        """Diferença de exatamente R$ 0.01 deve estar dentro da tolerância."""
        assert aplicar_tolerancia(200.00, 200.01) is True
        assert aplicar_tolerancia(200.01, 200.00) is True

    def test_diferenca_menor_que_um_centavo_esta_na_tolerancia(self):
        """Diferença de R$ 0.005 deve estar dentro da tolerância."""
        assert aplicar_tolerancia(100.005, 100.00) is True

    def test_diferenca_de_dois_centavos_fora_da_tolerancia(self):
        """Diferença de R$ 0.02 deve estar fora da tolerância."""
        assert aplicar_tolerancia(200.00, 200.02) is False
        assert aplicar_tolerancia(200.02, 200.00) is False

    def test_diferenca_de_um_real_fora_da_tolerancia(self):
        """Diferença de R$ 1.00 deve estar fora da tolerância."""
        assert aplicar_tolerancia(150.00, 151.00) is False

    def test_diferenca_negativa_grande_fora_da_tolerancia(self):
        """Diferença negativa maior que R$ 0.01 deve estar fora."""
        assert aplicar_tolerancia(100.00, 95.00) is False

    def test_internal_amount_nulo_retorna_false(self):
        """Se internal_amount for None, deve retornar False."""
        assert aplicar_tolerancia(None, 150.00) is False

    def test_processor_amount_nulo_retorna_false(self):
        """Se processor_amount for None, deve retornar False."""
        assert aplicar_tolerancia(150.00, None) is False

    def test_ambos_nulos_retorna_false(self):
        """Se ambos os valores forem None, deve retornar False."""
        assert aplicar_tolerancia(None, None) is False

    def test_valores_altos_com_diferenca_minima(self):
        """Valores altos com diferença dentro da tolerância."""
        assert aplicar_tolerancia(500000.00, 500000.01) is True

    def test_valores_zero_ambos(self):
        """Ambos os valores iguais a zero."""
        assert aplicar_tolerancia(0.0, 0.0) is True

    def test_valor_zero_e_um_centavo(self):
        """Um valor zero e outro 0.01 — dentro da tolerância."""
        assert aplicar_tolerancia(0.0, 0.01) is True