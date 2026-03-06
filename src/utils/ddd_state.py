import re

DDD_STATE = {
    "11": "SP","12": "SP","13": "SP","14": "SP","15": "SP","16": "SP","17": "SP","18": "SP","19": "SP",
    "21": "RJ","22": "RJ","24": "RJ",
    "27": "ES","28": "ES",
    "31": "MG","32": "MG","33": "MG","34": "MG","35": "MG","37": "MG","38": "MG",
    "41": "PR","42": "PR","43": "PR","44": "PR","45": "PR","46": "PR",
    "47": "SC","48": "SC","49": "SC",
    "51": "RS","53": "RS","54": "RS","55": "RS",
    "61": "DF",
    "62": "GO","64": "GO",
    "63": "TO",
    "65": "MT","66": "MT",
    "67": "MS",
    "68": "AC",
    "69": "RO",
    "71": "BA","73": "BA","74": "BA","75": "BA","77": "BA",
    "79": "SE",
    "81": "PE","87": "PE",
    "82": "AL",
    "83": "PB",
    "84": "RN",
    "85": "CE","88": "CE",
    "86": "PI","89": "PI",
    "91": "PA","93": "PA","94": "PA",
    "92": "AM","97": "AM",
    "95": "RR",
    "96": "AP",
    "98": "MA","99": "MA"
}

def clean_number(number: str) -> str:
    return re.sub(r"\D", "", str(number or ""))

def normalize_br_number(number: str) -> str:
    """
    Normaliza números brasileiros removendo:
    - prefixos de discagem: 0, 00, 000...
    - prefixo internacional 55 (incluindo 0055)
    Mantém apenas algo começando com DDD+numero (ex: 28999035087).
    """
    n = clean_number(number)

    if not n:
        return ""

    # remove prefixos 00/000... (discagem internacional)
    while n.startswith("00"):
        n = n[2:]

    # remove +55 / 55 (código do Brasil)
    if n.startswith("55") and len(n) >= 12:
        n = n[2:]

    # remove 0 na frente (tronco / prefixo operadora / discagem longa distância)
    # exemplo: 028999035087 -> remove o primeiro 0 => 28999035087
    if n.startswith("0") and len(n) >= 11:
        n = n[1:]

    return n

def extract_ddd(number: str):
    n = normalize_br_number(number)

    # precisamos de pelo menos DDD + 8/9 dígitos
    if len(n) < 10:
        return None

    ddd = n[:2]
    return ddd if ddd in DDD_STATE else None

def get_state(number: str):
    ddd = extract_ddd(number)
    return DDD_STATE.get(ddd) if ddd else None