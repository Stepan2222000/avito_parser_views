"""Сбор просмотров с Авито по артикулам запчастей.

Обвязка вокруг библиотеки `avito`: она умеет ходить и разбирать, а здесь живут
очереди задач, поставщик адресов и порядок обхода.

Библиотеки нет в PyPI, поэтому её клон лежит рядом (lib/avito_library) и путь к нему
добавляется здесь — до первого `import avito` в остальных модулях.
"""
from __future__ import annotations

import sys
from pathlib import Path

КОРЕНЬ = Path(__file__).resolve().parent.parent
_БИБЛИОТЕКА = КОРЕНЬ / "lib" / "avito_library"

if _БИБЛИОТЕКА.is_dir() and str(_БИБЛИОТЕКА) not in sys.path:
    sys.path.insert(0, str(_БИБЛИОТЕКА))
