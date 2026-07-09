"""Référentiel externe (simulé) enrichissant les catégories produit : équivalent
d'une taxonomie business récupérée hors du système source (ERP marketing),
utilisée pour peupler dwh.dim_category."""

CATEGORY_REFERENCE = {
    "Mode":         {"department": "Textile & Accessoires", "margin_target_pct": 45.0, "is_seasonal": True},
    "Electronique": {"department": "High-Tech",              "margin_target_pct": 15.0, "is_seasonal": False},
    "Maison":       {"department": "Habitat & Décoration",   "margin_target_pct": 35.0, "is_seasonal": False},
    "Sport":        {"department": "Loisirs & Sport",        "margin_target_pct": 30.0, "is_seasonal": True},
    "Beaute":       {"department": "Santé & Beauté",         "margin_target_pct": 50.0, "is_seasonal": False},
    "Jouets":       {"department": "Loisirs & Sport",        "margin_target_pct": 40.0, "is_seasonal": True},
}

# Valeurs par défaut pour une catégorie vue en staging mais absente du référentiel
# (évite de casser le pipeline si l'API introduit une nouvelle catégorie).
DEFAULT_CATEGORY_ENTRY = {"department": "Autre", "margin_target_pct": None, "is_seasonal": False}
