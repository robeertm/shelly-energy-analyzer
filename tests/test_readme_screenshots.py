"""The README is the shop window — every image on it must exist and be real.

A broken image on a public README is worse than no image, and an anchor that
scrolls nowhere is a dead link nobody notices in review. Both are mechanical
facts, so they get a test rather than a careful look.
"""
import os
import re
import sys

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))

README = open(os.path.join(ROOT, "README.md"), encoding="utf-8").read()
SHOTS = os.path.join(ROOT, "docs", "screenshots")


def _slug(heading: str) -> str:
    """GitHub's heading anchor: lowercase, drop anything that is not a word
    character/space/hyphen (emoji included), spaces to hyphens."""
    s = heading.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    return re.sub(r"\s", "-", s)


def test_every_image_the_readme_shows_exists():
    refs = re.findall(r"!\[[^\]]*\]\((docs/screenshots/[^)]+)\)", README)
    assert len(refs) > 30, f"only {len(refs)} images referenced — did the scan break?"
    missing = [r for r in refs if not os.path.exists(os.path.join(ROOT, r))]
    assert not missing, f"referenced but not in the repo: {missing}"
    print(f"OK  all {len(refs)} README images exist")


def test_the_new_ev_screenshots_are_on_the_readme():
    """Added in 16.80: the feature had no picture at all before."""
    for name in ("aurora_desktop_ev_log.png", "aurora_desktop_ev_curve.png",
                 "aurora_light_ev_log.png", "aurora_phone_ev_log.png",
                 "aurora_phone_ev_curve.png"):
        assert os.path.exists(os.path.join(SHOTS, name)), f"{name} is not in the repo"
        assert name in README, f"{name} is in the repo but on no page"
        size = os.path.getsize(os.path.join(SHOTS, name))
        assert size > 200_000, f"{name} is only {size} B — an error page, not a dashboard"
    print("OK  five EV screenshots, all on the README, all a plausible size")


# Thirteen files from the classic-skin era that no section links to any more.
# Pinned rather than deleted: removing images is not this change's business, but
# the list must not grow — a repo people clone should not carry pictures of
# nothing. Shrinking this list is always welcome; adding to it is a regression.
KNOWN_ORPHANS = {
    "desktop_01_live_dark.png", "desktop_02_costs_dark.png",
    "desktop_03_heatmap_dark.png", "desktop_06_co2_dark.png",
    "mobile_01_live_dark.png", "mobile_02_costs_dark.png",
    "mobile_03_heatmap_dark.png", "mobile_05_weather.png", "mobile_06_co2.png",
    "mobile_06_co2_dark.png", "mobile_07_anomalies.png", "mobile_12_goals.png",
    "mobile_14_nilm.png",
}


def test_no_new_screenshot_is_orphaned():
    """A file nobody links to is dead weight in a repo people clone."""
    on_page = set(re.findall(r"docs/screenshots/([A-Za-z0-9_.-]+)", README))
    on_disk = {f for f in os.listdir(SHOTS) if f.endswith(".png")}
    orphans = sorted(on_disk - on_page - KNOWN_ORPHANS)
    assert not orphans, f"{len(orphans)} new screenshot(s) no page shows: {orphans}"
    gone = sorted(KNOWN_ORPHANS - on_disk)
    assert not gone, f"the pinned list mentions files that no longer exist: {gone}"
    print(f"OK  {len(on_disk)} screenshots, no new orphan "
          f"({len(KNOWN_ORPHANS)} known from the classic era)")


def test_in_page_links_point_at_real_headings():
    anchors = {_slug(h) for h in re.findall(r"^#{1,6}\s+(.+)$", README, re.M)}
    links = re.findall(r"\]\(#([^)]+)\)", README)
    assert links, "no in-page links found — did the scan break?"
    broken = [l for l in links if l not in anchors]
    assert not broken, f"links to nowhere: {broken} (headings: {sorted(anchors)[:6]}…)"
    print(f"OK  {len(links)} in-page links, all resolve to a heading")


def test_the_readme_still_promises_only_what_demo_mode_can_show():
    """The page states every image comes from the built-in demo mode. That was
    true only because no page needing PV, a battery or a wallbox had a picture;
    16.80 gave demo mode all three so the sentence stays true."""
    assert "comes from the app's own demo mode" in README
    from shelly_analyzer.services.demo import default_demo_devices
    keys = {d.key for d in default_demo_devices()}
    assert {"demo3", "demo4"} <= keys, keys
    print("OK  the demo-mode promise on the README is still true")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    for f in fns:
        f()
    print(f"\n{len(fns)} Tests bestanden")
