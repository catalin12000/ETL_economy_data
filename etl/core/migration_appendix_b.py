from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Iterable

import pandas as pd
import pdfplumber


_MONTHS_FULL_GR = {
    "ΙΑΝΟΥΑΡΙΟΣ": 1,
    "ΦΕΒΡΟΥΑΡΙΟΣ": 2,
    "ΜΑΡΤΙΟΣ": 3,
    "ΑΠΡΙΛΙΟΣ": 4,
    "ΜΑΙΟΣ": 5,
    "ΙΟΥΝΙΟΣ": 6,
    "ΙΟΥΛΙΟΣ": 7,
    "ΑΥΓΟΥΣΤΟΣ": 8,
    "ΣΕΠΤΕΜΒΡΙΟΣ": 9,
    "ΟΚΤΩΒΡΙΟΣ": 10,
    "ΝΟΕΜΒΡΙΟΣ": 11,
    "ΔΕΚΕΜΒΡΙΟΣ": 12,
}

_MONTHS_ABBR_GR = {
    "ΙΑΝ": 1,
    "ΦΕΒ": 2,
    "ΜΑΡ": 3,
    "ΑΠΡ": 4,
    "ΜΑΙ": 5,
    "ΙΟΥΝ": 6,
    "ΙΟΥΛ": 7,
    "ΑΥΓ": 8,
    "ΣΕΠ": 9,
    "ΟΚΤ": 10,
    "ΝΟΕ": 11,
    "ΔΕΚ": 12,
}

_AREA_MAP = {
    "ΑΤΤΙΚΗΣ": "Attica",
    "ΜΑΚΕΔΟΝΙΑΣ ΘΡΑΚΗΣ": "Macedonia and Thrace",
    "ΠΕΛΟΠΟΝΝΗΣΟΥ ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ ΙΟΝΙΟΥ": "Peloponnese, Western Greece and the Ionian",
    "ΚΡΗΤΗΣ": "Crete",
    "ΘΕΣΣΑΛΙΑΣ ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ": "Thessaly and Central Greece",
    "ΑΙΓΑΙΟΥ": "Aegean",
    "ΗΠΕΙΡΟΥ ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ": "Epirus and Western Macedonia",
}

_COUNTRY_MAP = {
    "ΑΛΒΑΝΙΑ": "ALBANIA",
    "ΚΙΝΑ": "CHINA",
    "ΠΑΚΙΣΤΑΝ": "PAKISTAN",
    "ΓΕΩΡΓΙΑ": "GEORGIA",
    "ΜΠΑΝΓΚΛΑΝΤΕΣ": "BANGLADESH",
    "ΡΩΣΙΑ": "RUSSIA",
    "ΤΟΥΡΚΙΑ": "TURKEY",
    "ΑΙΓΥΠΤΟΣ": "EGYPT",
    "ΟΥΚΡΑΝΙΑ": "UKRAINE",
    "ΦΙΛΙΠΠΙΝΕΣ": "PHILIPPINES",
    "ΙΝΔΙΑ": "INDIA",
    "ΛΙΒΑΝΟΣ": "LEBANON",
    "ΙΡΑΝ": "IRAN",
    "ΙΡΑΚ": "IRAQ",
    "ΙΟΡΔΑΝΙΑ": "JORDAN",
    "ΣΥΡΙΑ": "SYRIA",
    "ΑΡΜΕΝΙΑ": "ARMENIA",
    "ΣΕΡΒΙΑ": "SERBIA",
    "ΙΣΡΑΗΛ": "ISRAEL",
    "ΧΟΝΓΚ ΚΟΝΓΚ": "HONG KONG",
    "ΒΙΕΤΝΑΜ": "VIETNAM",
    "ΗΝ ΒΑΣ Μ ΒΡΕΤΑΝΙΑΣ": "UNITED KINGDOM",
    "ΗΝ ΠΟΛΙΤΕΙΕΣ ΑΜΕΡΙΚΗΣ ΗΠΑ": "UNITED STATES",
    "ΑΛΛΕΣ ΙΘΑΓΕΝΕΙΕΣ": "OTHER NATIONALITIES",
    "ΑΛΛΕΣ": "OTHER",
}

_PERCENT_RE = re.compile(r"^\d+(?:,\d+)?%$")
_INT_RE = re.compile(r"^\d[\d.]*$")
_FULL_MONTH_RE = re.compile(
    r"\b(Ιανουάριος|Φεβρουάριος|Μάρτιος|Απρίλιος|Μάιος|Ιούνιος|Ιούλιος|Αύγουστος|Σεπτέμβριος|Οκτώβριος|Νοέμβριος|Δεκέμβριος)\s+(\d{4})\b",
    re.IGNORECASE,
)
_SHORT_MONTH_RE = re.compile(r"([Α-ΩΪΫάέήίόύώϊϋΐΰ]+)-(\d{2})", re.IGNORECASE)


def _norm(text: str) -> str:
    text = unicodedata.normalize("NFD", str(text or ""))
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.replace("–", " ").replace("-", " ").replace("&", " ")
    text = re.sub(r"[.,/%()]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip().upper()
    return text


def _clean_cell(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _parse_int(value: object) -> int | pd.NA:
    text = _clean_cell(value)
    if not text:
        return pd.NA
    text = text.replace(".", "").replace(",", "")
    if not text.isdigit():
        return pd.NA
    return int(text)


def _iter_table_rows(pdf_path: Path, page_number: int, table_index: int = 0) -> Iterable[list[str]]:
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[page_number]
        tables = page.extract_tables()
        if table_index >= len(tables):
            raise RuntimeError(f"Page {page_number + 1} missing table {table_index} in {pdf_path}")
        for row in tables[table_index]:
            yield [_clean_cell(cell) for cell in row]


def _extract_page_text(pdf_path: Path, page_number: int) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        return pdf.pages[page_number].extract_text() or ""


def _coerce_report_period(report_year: int | None, report_month: int | None, pdf_path: Path) -> tuple[int, int]:
    if report_year is not None and report_month is not None:
        return report_year, report_month

    text = _extract_page_text(pdf_path, 0)
    match = _FULL_MONTH_RE.search(text)
    if not match:
        raise RuntimeError(f"Could not detect report period in {pdf_path}")
    month_name = _norm(match.group(1))
    year = int(match.group(2))
    month = _MONTHS_FULL_GR[month_name]
    return year, month


def _parse_short_month_label(label: str) -> tuple[int, int] | None:
    match = _SHORT_MONTH_RE.search(_clean_cell(label))
    if not match:
        return None
    month_name = _norm(match.group(1))
    year = 2000 + int(match.group(2))
    month = _MONTHS_ABBR_GR.get(month_name)
    if not month:
        return None
    return year, month


def _parse_date_label(label: str) -> tuple[int, int] | None:
    text = _clean_cell(label)
    date_match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if date_match:
        return int(date_match.group(3)), int(date_match.group(2))
    return _parse_short_month_label(text)


def _map_area(label: str) -> str | None:
    norm = _norm(label)
    if not norm or norm == "Υ Μ Α":
        return None
    for greek, english in _AREA_MAP.items():
        if norm.startswith(greek) or greek.startswith(norm):
            return english
    return None


def _map_country(label: str, *, other_label: str) -> str:
    norm = _norm(label)
    if norm.startswith("ΑΛΛΕΣ"):
        return other_label
    for greek, english in _COUNTRY_MAP.items():
        if norm.startswith(greek) or greek.startswith(norm):
            if english in {"OTHER", "OTHER NATIONALITIES"}:
                return other_label
            return english
    raise RuntimeError(f"Unmapped country label: {label!r}")


def _extract_latest_aggregate_numbers(pdf_path: Path, report_year: int, report_month: int) -> tuple[int, int, int]:
    month_name = next(name for name, month in _MONTHS_FULL_GR.items() if month == report_month)
    text = _extract_page_text(pdf_path, 0)
    for raw_line in text.splitlines():
        line = _clean_cell(raw_line)
        if not line:
            continue
        if month_name in _norm(line) and str(report_year) in line:
            nums = re.findall(r"\d[\d.]*", line)
            if len(nums) >= 5:
                values = nums[-5:]
                return int(values[0].replace(".", "")), int(values[2].replace(".", "")), int(values[3].replace(".", ""))
    raise RuntimeError(f"Could not extract aggregate totals for {report_year}-{report_month:02d} from {pdf_path}")


def _extract_monthly_type_table(pdf_path: Path, page_number: int) -> pd.DataFrame:
    rows = list(_iter_table_rows(pdf_path, page_number, 0))
    extracted: list[dict[str, object]] = []
    current_type: str | None = None

    for row in rows:
        row_text = " ".join(part for part in row if part)
        row_norm = _norm(row_text)
        if "ΑΡΧΙΚΕΣ ΧΟΡΗΓΗΣΕΙΣ" in row_norm:
            current_type = "Initial"
            continue
        if "ΑΝΑΝΕΩΣΕΙΣ" in row_norm:
            current_type = "Renewal"
            continue
        if current_type is None:
            continue
        period = _parse_short_month_label(row[1] if len(row) > 1 else "")
        if not period:
            continue
        year, month = period
        extracted.append(
            {
                "Month": month,
                "Year": year,
                "Issued": _parse_int(row[4] if len(row) > 4 else ""),
                "Rejected": _parse_int(row[7] if len(row) > 7 else ""),
                "Revoked": _parse_int(row[10] if len(row) > 10 else ""),
                "Pending": _parse_int(row[13] if len(row) > 13 else ""),
                "Type": current_type,
            }
        )

    return pd.DataFrame(extracted)


def _extract_monthly_category_table(pdf_path: Path, page_number: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for type_name, table_index in [("Initial", 0), ("Renewal", 1)]:
        rows = list(_iter_table_rows(pdf_path, page_number, table_index))
        extracted: list[dict[str, object]] = []
        for row in rows:
            period = _parse_short_month_label(row[1] if len(row) > 1 else "")
            if not period:
                continue
            year, month = period
            extracted.append(
                {
                    "Month": month,
                    "Year": year,
                    "Work": _parse_int(row[4] if len(row) > 4 else ""),
                    "Other": _parse_int(row[7] if len(row) > 7 else ""),
                    "Family Re": _parse_int(row[10] if len(row) > 10 else ""),
                    "Studies": _parse_int(row[13] if len(row) > 13 else ""),
                    "Type": type_name,
                }
            )
        frames.append(pd.DataFrame(extracted))
    return pd.concat(frames, ignore_index=True)


def extract_residence_permits_aggregate(pdf_path: Path, report_year: int | None = None, report_month: int | None = None) -> pd.DataFrame:
    year, month = _coerce_report_period(report_year, report_month, pdf_path)
    eu_greek_origin, third_country_nationals, political_refugees = _extract_latest_aggregate_numbers(pdf_path, year, month)
    return pd.DataFrame(
        [
            {
                "Month": month,
                "Year": year,
                "Eu Citizens Of Greek Origin": eu_greek_origin,
                "Third Country Nationals": third_country_nationals,
                "Political Refugees": political_refugees,
            }
        ]
    )


def extract_residence_permits_current(pdf_path: Path, report_year: int | None = None, report_month: int | None = None) -> pd.DataFrame:
    rows = list(_iter_table_rows(pdf_path, 0, 2))
    extracted: list[dict[str, object]] = []
    for row in rows:
        period = _parse_date_label(row[1] if len(row) > 1 else "")
        if not period:
            continue
        year, month = period
        extracted.append(
            {
                "Month": month,
                "Year": year,
                "Work": _parse_int(row[4] if len(row) > 4 else ""),
                "Other": _parse_int(row[5] if len(row) > 5 else ""),
                "Family Reunification": _parse_int(row[7] if len(row) > 7 else ""),
                "Studies": _parse_int(row[9] if len(row) > 9 else ""),
            }
        )
    return pd.DataFrame(extracted)


def extract_residence_permits_application(pdf_path: Path, report_year: int | None = None, report_month: int | None = None) -> pd.DataFrame:
    return _extract_monthly_type_table(pdf_path, 2)


def extract_residence_permits_golden_visa(pdf_path: Path, report_year: int | None = None, report_month: int | None = None) -> pd.DataFrame:
    return _extract_monthly_type_table(pdf_path, 12)


def extract_residence_permits_issued(pdf_path: Path, report_year: int | None = None, report_month: int | None = None) -> pd.DataFrame:
    return _extract_monthly_category_table(pdf_path, 5)


def _extract_snapshot_top10_current(pdf_path: Path, report_year: int, report_month: int) -> pd.DataFrame:
    rows = list(_iter_table_rows(pdf_path, 1, 0))
    extracted: list[dict[str, object]] = []
    for row in rows:
        tokens = [token for token in row if token]
        if not tokens:
            continue
        if tokens[0].isdigit() and len(tokens) >= 6:
            rank = int(tokens[0])
            country = _map_country(tokens[1], other_label="OTHER NATIONALITIES")
            men = _parse_int(tokens[2])
            women = _parse_int(tokens[3])
            total = _parse_int(tokens[4])
        elif any("ΑΛΛΕΣ" in _norm(token) for token in tokens) and len(tokens) >= 5:
            rank = 11
            country = "OTHER NATIONALITIES"
            men = _parse_int(tokens[1])
            women = _parse_int(tokens[2])
            total = _parse_int(tokens[3])
        else:
            continue
        extracted.append(
            {
                "Month": report_month,
                "Year": report_year,
                "Rank": rank,
                "Country": country,
                "Permits_Granted_to_Men": men,
                "Permit_Granted_to_Women": women,
                "Total_Permits_Granted": total,
            }
        )
    return pd.DataFrame(extracted)


def extract_residence_permits_top10_countries(pdf_path: Path, report_year: int | None = None, report_month: int | None = None) -> pd.DataFrame:
    year, month = _coerce_report_period(report_year, report_month, pdf_path)
    return _extract_snapshot_top10_current(pdf_path, year, month)


def _extract_region_table_rows(pdf_path: Path, page_number: int, period_label: str, report_year: int, report_month: int) -> pd.DataFrame:
    rows = list(_iter_table_rows(pdf_path, page_number, 0))
    extracted: list[dict[str, object]] = []
    permit_type: str | None = None

    for row in rows:
        row_text = " ".join(part for part in row if part)
        row_norm = _norm(row_text)
        if "ΑΡΧΙΚΕΣ ΧΟΡΗΓΗΣΕΙΣ" in row_norm:
            permit_type = "Initial"
            continue
        if "ΑΝΑΝΕΩΣΕΙΣ" in row_norm:
            permit_type = "Renewal"
            continue
        if permit_type is None or "ΣΥΝΟΛΟ" in row_norm or "ΕΚΔΟΘΕΙΣΕΣ" in row_norm:
            continue

        numeric_positions = [idx for idx, cell in enumerate(row) if _INT_RE.match(cell or "")]
        if len(numeric_positions) < 4:
            continue

        first_numeric = numeric_positions[0]
        label = " ".join(part for part in row[:first_numeric] if part)
        area = _map_area(label)
        if not area:
            continue

        nums = [_parse_int(row[idx]) for idx in numeric_positions]
        if len(nums) >= 5:
            issued, rejected, revoked, pending = nums[0], nums[1], nums[2], nums[3]
        else:
            issued, rejected, revoked, pending = nums[0], nums[1], pd.NA, nums[2]

        extracted.append(
            {
                "Year": report_year,
                "Month": report_month,
                "Permit Type": permit_type,
                "Period": period_label,
                "Area": area,
                "Issued": issued,
                "Rejected": rejected,
                "Revoked": revoked,
                "Pending": pending,
            }
        )

    return pd.DataFrame(extracted)


def extract_geo_distribution_of_issued_and_pending_permits(pdf_path: Path, report_year: int | None = None, report_month: int | None = None) -> pd.DataFrame:
    year, month = _coerce_report_period(report_year, report_month, pdf_path)
    page4_text = _extract_page_text(pdf_path, 3)
    since_match = re.search(r"για τα έτη\s+(\d{4})-\d{4}", page4_text, re.IGNORECASE)
    since_start_year = int(since_match.group(1)) if since_match else year - 4
    cumulative = _extract_region_table_rows(pdf_path, 3, f"Since {since_start_year}", year, month)
    rolling = _extract_region_table_rows(pdf_path, 4, "12-months", year, month)
    return pd.concat([cumulative, rolling], ignore_index=True)


def _extract_golden_visa_top10_table(pdf_path: Path, page_number: int, applicant: str, report_year: int, report_month: int) -> pd.DataFrame:
    rows = list(_iter_table_rows(pdf_path, page_number, 0))
    extracted: list[dict[str, object]] = []
    for row in rows:
        tokens = [token for token in row if token]
        if not tokens:
            continue
        if tokens[0].isdigit() and len(tokens) >= 7:
            rank = int(tokens[0])
            renewal_country = _map_country(tokens[1], other_label="OTHER")
            renewal_permits = _parse_int(tokens[2])
            initial_country = _map_country(tokens[4], other_label="OTHER")
            initial_permits = _parse_int(tokens[5])
        elif any("ΑΛΛΕΣ" in _norm(token) for token in tokens) and len(tokens) >= 6:
            rank = 11
            renewal_country = "OTHER"
            renewal_permits = _parse_int(tokens[1])
            initial_country = "OTHER"
            initial_permits = _parse_int(tokens[4])
        else:
            continue

        extracted.append(
            {
                "Month": report_month,
                "Year": report_year,
                "Rank": rank,
                "Country": renewal_country,
                "Permits": renewal_permits,
                "Type": "Renewal",
                "Applicant": applicant,
            }
        )
        extracted.append(
            {
                "Month": report_month,
                "Year": report_year,
                "Rank": rank,
                "Country": initial_country,
                "Permits": initial_permits,
                "Type": "Initial",
                "Applicant": applicant,
            }
        )
    return pd.DataFrame(extracted)


def extract_residence_permits_top10_countries_golden_visa(
    pdf_path: Path, report_year: int | None = None, report_month: int | None = None
) -> pd.DataFrame:
    year, month = _coerce_report_period(report_year, report_month, pdf_path)
    investors = _extract_golden_visa_top10_table(pdf_path, 9, "Investor", year, month)
    family = _extract_golden_visa_top10_table(pdf_path, 10, "Family of investor", year, month)
    return pd.concat([investors, family], ignore_index=True)
