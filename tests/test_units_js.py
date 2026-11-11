# -*- coding: utf-8 -*-
"""static/units.js swaps unit tokens for the language's own spelling."""
import json
import os
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from shelly_analyzer import i18n  # noqa: E402

JS = os.path.join(os.path.dirname(__file__), "..", "src", "shelly_analyzer", "web", "static", "units.js")
pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")

HARNESS = """
global.window = {}; global.MutationObserver = function(){ this.observe = function(){}; };
global.document = {readyState:'complete', documentElement:{nodeType:1,nodeName:'HTML',childNodes:[]}, addEventListener(){}};
global.I18N = JSON.parse(process.argv[1]);
require(process.argv[2]);
const out = JSON.parse(process.argv[3]).map(s => window.seaUnits.loc(s));
console.log(JSON.stringify(out));
"""


def _run(lang, samples):
    table = i18n.get_lang_map(lang, "web.unit.")
    res = subprocess.run(["node", "-e", HARNESS, json.dumps(table), JS, json.dumps(samples)],
                         capture_output=True, text=True, check=True)
    return json.loads(res.stdout)


def test_russian_units():
    got = _run("ru", ["12.3 kWh", "230 V", "1h", "7d", "12 ct/kWh", "g/kWh", "5 kg CO₂", "3.4 KB/s", "Hello World"])
    assert got == ["12.3 кВт·ч", "230 В", "1ч", "7д", "12 цент/кВт·ч", "г/кВт·ч", "5 кг CO₂", "3.4 КБ/с", "Hello World"]


def test_english_is_untouched():
    samples = ["12.3 kWh", "230 V", "1h", "g/kWh"]
    assert _run("en", samples) == samples


def test_french_writes_octets():
    assert _run("fr", ["5 MB", "12 B/s"]) == ["5 Mo", "12 o/s"]
