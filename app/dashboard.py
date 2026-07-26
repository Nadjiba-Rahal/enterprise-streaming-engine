"""Multilingual business command center for the in-memory fraud engine."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

try:
    from app.generator import StochasticEventGenerator
    from app.pipeline import StreamingPipeline
except ModuleNotFoundError:  # Allows `streamlit run app/dashboard.py` from inside app/.
    from generator import StochasticEventGenerator
    from pipeline import StreamingPipeline


st.set_page_config(
    page_title="Commerce Risk Command Center",
    page_icon="shield",
    layout="wide",
    initial_sidebar_state="expanded",
)


LANGUAGES = {
    "English": "en",
    "Français": "fr",
    "العربية": "ar",
}


TEXT = {
    "en": {
        "demo_controls": "Demo controls",
        "language": "Language",
        "run_live": "Run live simulation",
        "demo_pace": "Demo pace",
        "refresh_every": "Refresh every",
        "pace_help": "Lower pace keeps the dashboard readable for business demos.",
        "show_scenario": "Show a business scenario",
        "risky_order": "Risky order",
        "demand_surge": "Demand surge",
        "abandoned_cart": "Abandoned cart",
        "low_stock": "Low stock alert",
        "manual_event": "Manual event",
        "business_event": "Business event",
        "category": "Category",
        "order_value": "Order value",
        "customer_location": "Customer location",
        "order_value_disabled_help": "Only checkout/card events carry a monetary amount.",
        "impossible_travel": "Mark as impossible travel",
        "add_event": "Add event",
        "scenario_added": "Scenario added",
        "event_added": "Event added",
        "reset_demo": "Reset demo",
        "reset_done": "Demo state cleared",
        "download_fraud_csv": "Download fraud feed (CSV)",
        "download_decisions_csv": "Download action plan (CSV)",
        "live": "Live demo running",
        "paused": "Demo paused",
        "eyebrow": "Retail protection and revenue intelligence",
        "title": "Commerce Risk Command Center",
        "subtitle": "A sellable business dashboard that turns live store activity into clear actions: block risky orders, recover carts, adjust prices, restock inventory, and protect checkout reliability.",
        "sales": "Sales protected",
        "approved_gmv": "approved GMV",
        "customers": "Customers seen",
        "live_users": "live users",
        "blocked_orders": "Orders blocked",
        "risk_level": "Risk level",
        "avg_score": "average score",
        "service_health": "Service health",
        "exec_plan": "Action plan",
        "exec_caption": "What the business owner should do now, written as operational decisions instead of technical metrics.",
        "tabs": ["Action Plan", "Fraud", "Pricing", "Cart Recovery", "Inventory", "Health"],
        "fraud_tag": "Fraud",
        "revenue_tag": "Revenue",
        "inventory_tag": "Inventory",
        "ops_tag": "Operations",
        "retention_tag": "Retention",
        "fraud_quiet": "Fraud quiet",
        "fraud_quiet_detail": "No high-risk transaction crossed the block threshold. Keep accepting orders normally.",
        "review_blocked": "Review blocked orders",
        "review_blocked_detail": "{count} order(s) were blocked. Action: check customer identity, verify payment, and keep fulfilment paused until reviewed.",
        "no_markup": "Keep prices stable",
        "no_markup_detail": "Demand is steady. Action: keep current prices and continue monitoring conversion.",
        "raise_price": "Raise {category}",
        "raise_price_detail": "{views} views/min detected. Action: apply +{markup}% markup for 30 minutes, then watch conversion before making it permanent.",
        "inventory_stable": "Inventory stable",
        "inventory_stable_detail": "No category is below the reorder threshold. Action: no purchase order needed now.",
        "restock": "Restock {category}",
        "restock_detail": "Only {stock} units left. Action: create a purchase order today and pause promotions for this category.",
        "platform_healthy": "Platform healthy",
        "service_alarm": "Checkout issue",
        "service_alarm_detail": "{rate}% failed event rate. Action: ask operations to inspect checkout/payment errors before running campaigns.",
        "service_ok_detail": "{rate}% failed event rate. Action: checkout is safe for normal selling.",
        "recover_carts": "Recover abandoned carts",
        "recover_carts_detail": "{count} high-intent cart(s) are ready. Action: send the suggested offer now and follow up by email or WhatsApp.",
        "no_carts": "No recovery action",
        "no_carts_detail": "No cart has aged past the recovery threshold. Action: do nothing for now.",
        "fraud_title": "Fraud decisions",
        "fraud_caption": "What was blocked, why it was blocked, and what a manager should do next.",
        "no_urgent_fraud": "No urgent fraud cases",
        "no_urgent_fraud_detail": "The engine has not seen a transaction risky enough to require manual review.",
        "latest_risky": "Review the latest risky order",
        "latest_risky_detail": "Risk score {score:.2f} from {location}. Action: verify identity and payment before shipping. Reason: {reasons}.",
        "pricing_title": "Pricing suggestions",
        "pricing_caption": "Only shows categories where demand is strong enough to justify a business decision.",
        "no_pricing": "No pricing action yet",
        "no_pricing_detail": "Waiting for enough product views to estimate demand.",
        "pricing_keep": "Keep current prices",
        "pricing_keep_detail": "Traffic is healthy but not high enough to justify a markup.",
        "increase_category": "Increase {category} by {markup}%",
        "increase_category_detail": "{views} category views in the last minute. Action: test the markup for a short window and watch abandoned carts.",
        "recovery_title": "Cart recovery",
        "recovery_caption": "Turns abandoned carts into practical retention offers.",
        "send_offers": "Send recovery offers",
        "send_offers_detail": "{count} session(s) are ready for a recovery message. Action: send the offer and stop after one reminder.",
        "inventory_title": "Inventory actions",
        "inventory_caption": "Highlights categories that could stop converting because stock is too low.",
        "inventory_safe": "Inventory is safe",
        "inventory_safe_detail": "Every category is above the reorder threshold.",
        "reorder_category": "Reorder {category}",
        "reorder_category_detail": "Stock is down to {stock} units. Action: reorder and reduce ads until stock is replenished.",
        "health_title": "Service health",
        "health_caption": "Keeps the business user aware of checkout reliability without overwhelming them with logs.",
        "health_good": "Checkout flow looks healthy",
        "health_bad": "Technical issue needs attention",
        "show_technical": "Show technical stream details",
        "footer": "Standalone in-memory demo using asyncio.Queue and DuckDB :memory:.",
        "columns": {
            "event_ts": "Time",
            "risk_score": "Risk",
            "risk_reasons": "Why flagged",
            "user_id": "Customer",
            "ip_address": "IP",
            "location": "Location",
            "action": "Action",
            "amount": "Value",
            "blocked_orders": "Blocked orders",
            "worst_score": "Worst score",
            "category": "Category",
            "views_60s": "Views last minute",
            "suggested_markup_pct": "Suggested markup %",
            "session_id": "Session",
            "age_seconds": "Cart age seconds",
            "offer_payload": "Suggested offer",
            "stock": "Stock left",
            "threshold": "Alert level",
            "status": "Status",
        },
    },
    "fr": {
        "demo_controls": "Commandes de démonstration",
        "language": "Langue",
        "run_live": "Lancer la simulation",
        "demo_pace": "Vitesse de démonstration",
        "refresh_every": "Rafraîchir toutes les",
        "pace_help": "Une vitesse basse rend la démonstration plus lisible.",
        "show_scenario": "Afficher un scénario métier",
        "risky_order": "Commande risquée",
        "demand_surge": "Pic de demande",
        "abandoned_cart": "Panier abandonné",
        "low_stock": "Alerte stock bas",
        "manual_event": "Événement manuel",
        "business_event": "Événement métier",
        "category": "Catégorie",
        "order_value": "Valeur de commande",
        "customer_location": "Localisation client",
        "order_value_disabled_help": "Seuls les événements de paiement/carte ont un montant.",
        "impossible_travel": "Marquer comme voyage impossible",
        "add_event": "Ajouter l'événement",
        "scenario_added": "Scénario ajouté",
        "reset_demo": "Réinitialiser la démo",
        "reset_done": "État de la démo réinitialisé",
        "download_fraud_csv": "Télécharger le flux fraude (CSV)",
        "download_decisions_csv": "Télécharger le plan d'action (CSV)",
        "event_added": "Événement ajouté",
        "live": "Démo en direct",
        "paused": "Démo en pause",
        "eyebrow": "Protection retail et intelligence revenu",
        "title": "Centre de Décision Commerce",
        "subtitle": "Un tableau de bord métier vendable qui transforme l'activité live de la boutique en actions claires : bloquer les commandes risquées, récupérer les paniers, ajuster les prix, réapprovisionner et protéger le paiement.",
        "sales": "Ventes protégées",
        "approved_gmv": "GMV validé",
        "customers": "Clients vus",
        "live_users": "clients actifs",
        "blocked_orders": "Commandes bloquées",
        "risk_level": "Niveau de risque",
        "avg_score": "score moyen",
        "service_health": "Santé du service",
        "exec_plan": "Plan d'action",
        "exec_caption": "Ce que le dirigeant doit faire maintenant, formulé en décisions opérationnelles.",
        "tabs": ["Plan d'action", "Fraude", "Prix", "Relance panier", "Stock", "Santé"],
        "fraud_tag": "Fraude",
        "revenue_tag": "Revenu",
        "inventory_tag": "Stock",
        "ops_tag": "Opérations",
        "retention_tag": "Rétention",
        "fraud_quiet": "Fraude calme",
        "fraud_quiet_detail": "Aucune transaction à haut risque. Action : continuer à accepter les commandes normalement.",
        "review_blocked": "Vérifier les commandes bloquées",
        "review_blocked_detail": "{count} commande(s) bloquée(s). Action : vérifier l'identité, contrôler le paiement et suspendre l'expédition.",
        "no_markup": "Garder les prix stables",
        "no_markup_detail": "La demande est stable. Action : conserver les prix actuels et suivre la conversion.",
        "raise_price": "Augmenter {category}",
        "raise_price_detail": "{views} vues/min détectées. Action : appliquer +{markup}% pendant 30 minutes puis surveiller la conversion.",
        "inventory_stable": "Stock stable",
        "inventory_stable_detail": "Aucune catégorie sous le seuil. Action : aucune commande fournisseur nécessaire.",
        "restock": "Réapprovisionner {category}",
        "restock_detail": "Seulement {stock} unités restantes. Action : créer une commande fournisseur aujourd'hui et réduire les promotions.",
        "platform_healthy": "Plateforme saine",
        "service_alarm": "Problème de paiement",
        "service_alarm_detail": "{rate}% d'événements échoués. Action : demander aux opérations de vérifier le paiement avant toute campagne.",
        "service_ok_detail": "{rate}% d'événements échoués. Action : le paiement est sûr pour vendre normalement.",
        "recover_carts": "Récupérer les paniers abandonnés",
        "recover_carts_detail": "{count} panier(s) à forte intention. Action : envoyer l'offre recommandée maintenant.",
        "no_carts": "Aucune relance",
        "no_carts_detail": "Aucun panier n'a dépassé le seuil d'abandon. Action : ne rien faire pour l'instant.",
        "fraud_title": "Décisions fraude",
        "fraud_caption": "Ce qui a été bloqué, pourquoi, et l'action à prendre.",
        "no_urgent_fraud": "Aucun cas urgent",
        "no_urgent_fraud_detail": "Aucune transaction ne nécessite une revue manuelle.",
        "latest_risky": "Vérifier la dernière commande risquée",
        "latest_risky_detail": "Score {score:.2f} depuis {location}. Action : vérifier identité et paiement avant expédition. Raison : {reasons}.",
        "pricing_title": "Suggestions de prix",
        "pricing_caption": "Affiche seulement les catégories où la demande justifie une décision.",
        "no_pricing": "Aucune action prix",
        "no_pricing_detail": "Le système attend assez de vues produit pour estimer la demande.",
        "pricing_keep": "Garder les prix actuels",
        "pricing_keep_detail": "Le trafic est sain mais pas assez élevé pour augmenter les prix.",
        "increase_category": "Augmenter {category} de {markup}%",
        "increase_category_detail": "{views} vues catégorie sur la dernière minute. Action : tester brièvement et surveiller les paniers abandonnés.",
        "recovery_title": "Relance panier",
        "recovery_caption": "Transforme les paniers abandonnés en offres de rétention concrètes.",
        "send_offers": "Envoyer les offres",
        "send_offers_detail": "{count} session(s) prêtes. Action : envoyer l'offre et limiter à un rappel.",
        "inventory_title": "Actions stock",
        "inventory_caption": "Met en avant les catégories qui peuvent perdre des ventes par manque de stock.",
        "inventory_safe": "Stock sûr",
        "inventory_safe_detail": "Toutes les catégories sont au-dessus du seuil de réapprovisionnement.",
        "reorder_category": "Commander {category}",
        "reorder_category_detail": "Stock à {stock} unités. Action : recommander et réduire les publicités jusqu'au réassort.",
        "health_title": "Santé du service",
        "health_caption": "Explique la fiabilité du paiement sans noyer l'utilisateur dans les logs.",
        "health_good": "Parcours paiement sain",
        "health_bad": "Problème technique à traiter",
        "show_technical": "Afficher les détails techniques",
        "footer": "Démo autonome en mémoire avec asyncio.Queue et DuckDB :memory:.",
        "columns": {},
    },
    "ar": {
        "demo_controls": "أدوات العرض",
        "language": "اللغة",
        "run_live": "تشغيل المحاكاة",
        "demo_pace": "سرعة العرض",
        "refresh_every": "التحديث كل",
        "pace_help": "السرعة الهادئة تجعل القرار أوضح لصاحب العمل.",
        "show_scenario": "اعرض سيناريو تجاري",
        "risky_order": "طلب خطير",
        "demand_surge": "ارتفاع الطلب",
        "abandoned_cart": "سلة متروكة",
        "low_stock": "تنبيه نقص المخزون",
        "manual_event": "حدث يدوي",
        "business_event": "نوع الحدث",
        "category": "الفئة",
        "order_value": "قيمة الطلب",
        "customer_location": "موقع العميل",
        "order_value_disabled_help": "فقط أحداث الدفع/البطاقة لها قيمة مالية.",
        "impossible_travel": "اعتباره انتقالا مستحيلا",
        "add_event": "إضافة الحدث",
        "scenario_added": "تمت إضافة السيناريو",
        "reset_demo": "إعادة تعيين العرض",
        "reset_done": "تمت إعادة تعيين حالة العرض",
        "download_fraud_csv": "تحميل قائمة الاحتيال (CSV)",
        "download_decisions_csv": "تحميل خطة العمل (CSV)",
        "event_added": "تمت إضافة الحدث",
        "live": "العرض يعمل الآن",
        "paused": "العرض متوقف",
        "eyebrow": "حماية المتجر وزيادة الإيرادات",
        "title": "مركز قرارات التجارة",
        "subtitle": "لوحة أعمال قابلة للبيع تحول نشاط المتجر المباشر إلى قرارات واضحة: إيقاف الطلبات الخطيرة، استرجاع السلال، تعديل الأسعار، إعادة التخزين، وحماية الدفع.",
        "sales": "المبيعات المحمية",
        "approved_gmv": "مبيعات مقبولة",
        "customers": "العملاء",
        "live_users": "عملاء نشطون",
        "blocked_orders": "طلبات موقوفة",
        "risk_level": "مستوى الخطر",
        "avg_score": "متوسط الخطر",
        "service_health": "صحة الخدمة",
        "exec_plan": "خطة العمل",
        "exec_caption": "ما يجب على صاحب العمل فعله الآن بلغة قرارات وليس أرقام تقنية.",
        "tabs": ["خطة العمل", "الاحتيال", "التسعير", "استرجاع السلال", "المخزون", "الصحة"],
        "fraud_tag": "احتيال",
        "revenue_tag": "إيرادات",
        "inventory_tag": "مخزون",
        "ops_tag": "تشغيل",
        "retention_tag": "استرجاع",
        "fraud_quiet": "لا يوجد خطر كبير",
        "fraud_quiet_detail": "لا توجد معاملات عالية الخطورة. الإجراء: استمر في قبول الطلبات بشكل عادي.",
        "review_blocked": "راجع الطلبات الموقوفة",
        "review_blocked_detail": "تم إيقاف {count} طلب. الإجراء: تحقق من هوية العميل والدفع ولا تشحن قبل المراجعة.",
        "no_markup": "أبق الأسعار كما هي",
        "no_markup_detail": "الطلب مستقر. الإجراء: لا ترفع السعر الآن وراقب التحويل.",
        "raise_price": "ارفع سعر {category}",
        "raise_price_detail": "{views} مشاهدة في الدقيقة. الإجراء: ارفع السعر +{markup}% لمدة 30 دقيقة ثم راقب التحويل.",
        "inventory_stable": "المخزون مستقر",
        "inventory_stable_detail": "لا توجد فئة تحت حد التنبيه. الإجراء: لا حاجة لطلب شراء الآن.",
        "restock": "أعد تخزين {category}",
        "restock_detail": "بقي {stock} فقط. الإجراء: أنشئ طلب شراء اليوم وقلل الترويج لهذه الفئة.",
        "platform_healthy": "النظام يعمل جيدا",
        "service_alarm": "مشكلة في الدفع",
        "service_alarm_detail": "نسبة الفشل {rate}%. الإجراء: اطلب من فريق التشغيل فحص الدفع قبل أي حملة.",
        "service_ok_detail": "نسبة الفشل {rate}%. الإجراء: الدفع آمن للبيع العادي.",
        "recover_carts": "استرجع السلال المتروكة",
        "recover_carts_detail": "{count} سلة جاهزة. الإجراء: أرسل العرض المقترح الآن.",
        "no_carts": "لا توجد سلال للاسترجاع",
        "no_carts_detail": "لا توجد سلة تجاوزت حد التخلي. الإجراء: لا تفعل شيئا الآن.",
        "fraud_title": "قرارات الاحتيال",
        "fraud_caption": "ما الذي تم إيقافه ولماذا وما هو القرار التالي.",
        "no_urgent_fraud": "لا توجد حالات عاجلة",
        "no_urgent_fraud_detail": "لا توجد معاملة تحتاج مراجعة يدوية.",
        "latest_risky": "راجع آخر طلب خطير",
        "latest_risky_detail": "درجة الخطر {score:.2f} من {location}. الإجراء: تحقق من الهوية والدفع قبل الشحن. السبب: {reasons}.",
        "pricing_title": "اقتراحات التسعير",
        "pricing_caption": "يعرض فقط الفئات التي تستحق قرارا تجاريا.",
        "no_pricing": "لا يوجد قرار سعر الآن",
        "no_pricing_detail": "النظام ينتظر مشاهدات أكثر لتقدير الطلب.",
        "pricing_keep": "حافظ على الأسعار",
        "pricing_keep_detail": "الحركة جيدة لكنها لا تبرر رفع السعر.",
        "increase_category": "ارفع {category} بنسبة {markup}%",
        "increase_category_detail": "{views} مشاهدة خلال دقيقة. الإجراء: اختبر الزيادة لفترة قصيرة وراقب السلال المتروكة.",
        "recovery_title": "استرجاع السلال",
        "recovery_caption": "يحول السلال المتروكة إلى عروض عملية.",
        "send_offers": "أرسل عروض الاسترجاع",
        "send_offers_detail": "{count} جلسة جاهزة. الإجراء: أرسل العرض ولا تكرر أكثر من تذكير واحد.",
        "inventory_title": "قرارات المخزون",
        "inventory_caption": "يبين الفئات التي قد تفقد مبيعات بسبب نقص المخزون.",
        "inventory_safe": "المخزون آمن",
        "inventory_safe_detail": "كل الفئات فوق حد إعادة الطلب.",
        "reorder_category": "اطلب {category}",
        "reorder_category_detail": "المخزون {stock} وحدة. الإجراء: أعد الطلب وقلل الإعلانات حتى يصل المخزون.",
        "health_title": "صحة الخدمة",
        "health_caption": "يعرض موثوقية الدفع بدون تفاصيل تقنية مزعجة.",
        "health_good": "مسار الدفع سليم",
        "health_bad": "مشكلة تقنية تحتاج متابعة",
        "show_technical": "عرض التفاصيل التقنية",
        "footer": "عرض مستقل داخل الذاكرة باستخدام asyncio.Queue و DuckDB :memory:.",
        "columns": {},
    },
}

TEXT["fr"]["columns"] = {
    "event_ts": "Heure",
    "risk_score": "Risque",
    "risk_reasons": "Pourquoi",
    "user_id": "Client",
    "ip_address": "IP",
    "location": "Lieu",
    "action": "Action",
    "amount": "Valeur",
    "blocked_orders": "Commandes bloquées",
    "worst_score": "Score max",
    "category": "Catégorie",
    "views_60s": "Vues dernière minute",
    "suggested_markup_pct": "Hausse suggérée %",
    "session_id": "Session",
    "age_seconds": "Âge panier secondes",
    "offer_payload": "Offre suggérée",
    "stock": "Stock restant",
    "threshold": "Seuil",
    "status": "Statut",
}
TEXT["ar"]["columns"] = {
    "event_ts": "الوقت",
    "risk_score": "الخطر",
    "risk_reasons": "السبب",
    "user_id": "العميل",
    "ip_address": "IP",
    "location": "الموقع",
    "action": "الإجراء",
    "amount": "القيمة",
    "blocked_orders": "طلبات موقوفة",
    "worst_score": "أعلى خطر",
    "category": "الفئة",
    "views_60s": "مشاهدات آخر دقيقة",
    "suggested_markup_pct": "زيادة مقترحة %",
    "session_id": "الجلسة",
    "age_seconds": "عمر السلة بالثواني",
    "offer_payload": "العرض المقترح",
    "stock": "المخزون",
    "threshold": "حد التنبيه",
    "status": "الحالة",
}


def tr(lang: str, key: str, **kwargs: Any) -> str:
    value = TEXT[lang].get(key, TEXT["en"][key])
    if isinstance(value, list):
        return str(value)
    return str(value).format(**kwargs)


def tabs_for(lang: str) -> list[str]:
    return list(TEXT[lang]["tabs"])


def inject_css(lang: str) -> None:
    direction = "rtl" if lang == "ar" else "ltr"
    align = "right" if lang == "ar" else "left"
    st.markdown(
        f"""
        <style>
        :root {{
            --ink: #1f2933;
            --muted: #667085;
            --line: #dbe5ec;
            --accent: #21736f;
            --accent-soft: #e7f5f2;
            --danger: #b42318;
            --danger-soft: #fff0ed;
            --success: #067647;
            --success-soft: #e9f7ef;
            --warning: #a15c07;
            --warning-soft: #fff7e8;
        }}
        .stApp {{
            background: linear-gradient(180deg, #fbfcfd 0%, #eff5f6 100%);
            color: var(--ink);
            direction: {direction};
        }}
        [data-testid="stSidebar"] {{
            background: #f7fafc;
            border-right: 1px solid var(--line);
        }}
        [data-testid="stSidebar"] * {{
            color: var(--ink);
        }}
        .block-container {{
            padding-top: 1.05rem;
            max-width: 1420px;
        }}
        h1, h2, h3, p {{
            letter-spacing: 0;
            text-align: {align};
        }}
        .hero {{
            display: flex;
            align-items: flex-end;
            justify-content: space-between;
            gap: 1rem;
            padding: 1.15rem 1.2rem;
            margin-bottom: 1rem;
            background: linear-gradient(135deg, #ffffff 0%, #eaf5f2 100%);
            border: 1px solid var(--line);
            border-radius: 8px;
            box-shadow: 0 12px 30px rgba(31, 41, 51, 0.06);
        }}
        .eyebrow {{
            color: var(--accent);
            font-size: 0.76rem;
            font-weight: 800;
            text-transform: uppercase;
        }}
        .title {{
            margin-top: 0.16rem;
            font-size: 2rem;
            line-height: 1.08;
            font-weight: 850;
        }}
        .subtitle {{
            color: var(--muted);
            max-width: 900px;
            margin-top: 0.42rem;
            font-size: 0.98rem;
        }}
        .state-pill {{
            min-width: 170px;
            text-align: center;
            padding: 0.62rem 0.8rem;
            border-radius: 999px;
            border: 1px solid var(--line);
            background: #ffffff;
            font-weight: 800;
            color: var(--accent);
        }}
        .decision-grid {{
            display: grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap: 0.78rem;
            margin: 0.8rem 0 1rem 0;
        }}
        .decision-card {{
            background: rgba(255,255,255,0.96);
            border: 1px solid var(--line);
            border-radius: 8px;
            padding: 0.95rem 1rem;
            min-height: 148px;
            box-shadow: 0 10px 24px rgba(31, 41, 51, 0.04);
            text-align: {align};
        }}
        .decision-card strong {{
            display: block;
            color: var(--ink);
            font-size: 0.98rem;
            margin-bottom: 0.26rem;
        }}
        .decision-card span {{
            display: block;
            color: var(--muted);
            font-size: 0.86rem;
            line-height: 1.4;
        }}
        .tag {{
            display: inline-block;
            margin-bottom: 0.5rem;
            padding: 0.18rem 0.5rem;
            border-radius: 999px;
            background: var(--accent-soft);
            color: var(--accent);
            font-size: 0.72rem;
            font-weight: 800;
        }}
        .warning .tag {{ background: var(--warning-soft); color: var(--warning); }}
        .danger .tag {{ background: var(--danger-soft); color: var(--danger); }}
        .success .tag {{ background: var(--success-soft); color: var(--success); }}
        .action-box, .business-note {{
            background: #ffffff;
            border: 1px solid var(--line);
            border-left: 5px solid var(--accent);
            border-radius: 8px;
            padding: 0.84rem 0.95rem;
            margin-bottom: 0.72rem;
            box-shadow: 0 8px 18px rgba(31, 41, 51, 0.035);
            text-align: {align};
        }}
        .action-box strong, .business-note strong {{
            display: block;
            margin-bottom: 0.2rem;
        }}
        .action-box span, .business-note span {{
            color: var(--muted);
            font-size: 0.91rem;
            line-height: 1.42;
        }}
        .danger {{ border-left-color: var(--danger); }}
        .warning {{ border-left-color: var(--warning); }}
        .success {{ border-left-color: var(--success); }}
        [data-testid="stMetric"] {{
            background: #ffffff;
            border: 1px solid var(--line);
            border-radius: 8px;
            padding: 0.86rem 0.95rem;
            box-shadow: 0 9px 22px rgba(31, 41, 51, 0.04);
        }}
        [data-testid="stMetricValue"] {{
            color: var(--ink);
            font-size: 1.34rem;
        }}
        [data-testid="stMetricLabel"] {{
            color: var(--muted);
            font-weight: 750;
        }}
        .section-caption {{
            color: var(--muted);
            margin: -0.3rem 0 0.75rem 0;
            text-align: {align};
        }}
        .stTabs [data-baseweb="tab-list"] {{
            gap: 0.25rem;
            border-bottom: 1px solid var(--line);
        }}
        .stTabs [data-baseweb="tab"] {{
            height: 2.65rem;
            padding: 0 0.95rem;
            border-radius: 7px 7px 0 0;
            font-weight: 800;
        }}
        .stDataFrame {{
            border: 1px solid var(--line);
            border-radius: 8px;
            overflow: hidden;
        }}
        button[kind="primary"] {{
            background: var(--accent);
            border: 1px solid var(--accent);
        }}
        @media (max-width: 1200px) {{
            .decision-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
        }}
        @media (max-width: 680px) {{
            .hero {{ display: block; }}
            .state-pill {{ margin-top: 0.8rem; }}
            .decision-grid {{ grid-template-columns: 1fr; }}
            .title {{ font-size: 1.55rem; }}
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def get_pipeline() -> StreamingPipeline:
    if "pipeline" not in st.session_state:
        st.session_state.pipeline = StreamingPipeline()
    return st.session_state.pipeline


def get_generator() -> StochasticEventGenerator:
    if "generator" not in st.session_state:
        st.session_state.generator = StochasticEventGenerator(seed=42, anomaly_rate=0.055)
    return st.session_state.generator


async def publish_and_drain(pipeline: StreamingPipeline, events: list[dict[str, Any]]) -> None:
    for event in events:
        await pipeline.publish(event)
    await pipeline.drain()


def simulate_tick(pipeline: StreamingPipeline, generator: StochasticEventGenerator, lam: float) -> None:
    events = [generator.generate_event() for _ in range(min(generator.poisson_batch_size(lam), 10))]
    asyncio.run(publish_and_drain(pipeline, events))


def inject_events(pipeline: StreamingPipeline, events: list[dict[str, Any]]) -> None:
    asyncio.run(publish_and_drain(pipeline, events))


def euro(value: float) -> str:
    return f"EUR {value:,.0f}"


def pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def make_event(
    action: str,
    category: str = "electronics",
    amount: float = 0.0,
    user_id: str = "demo_customer",
    session_id: str = "demo_session",
    ip_address: str = "203.0.113.42",
    location: str = "Paris",
    previous_location: str = "",
    seconds_since_location_change: int | None = None,
    status: str = "ok",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "event_id": f"demo_{int(time.time() * 1000)}_{action}_{abs(hash((action, amount, time.time())))}",
        "event_ts": now.isoformat(),
        "user_id": user_id,
        "session_id": session_id,
        "ip_address": ip_address,
        "location": location,
        "previous_location": previous_location,
        "seconds_since_location_change": seconds_since_location_change,
        "product_id": "sku_demo",
        "category": category,
        "action": action,
        "amount": amount,
        "quantity": 1 if action == "checkout_completed" else 0,
        "status": status,
        "device": "desktop",
        "segment": "business_demo",
        "bot_likelihood": 0.35,
        "anomaly_type": "business_demo",
        "is_injected_anomaly": action == "card_velocity" or amount > 1000 or bool(previous_location),
        "checkout_failures_30s": 1 if action == "checkout_failed" else 0,
    }


def choose_language() -> str:
    selected = st.sidebar.radio(
        "Language / Langue / اللغة",
        list(LANGUAGES),
        horizontal=True,
        index=0,
    )
    return LANGUAGES[selected]


def sidebar_controls(pipeline: StreamingPipeline, generator: StochasticEventGenerator, lang: str) -> tuple[bool, float, float]:
    st.sidebar.header(tr(lang, "demo_controls"))
    st.sidebar.caption(tr(lang, "language"))
    running = st.sidebar.toggle(tr(lang, "run_live"), value=st.session_state.get("running", True), key="running")
    lam = st.sidebar.slider(tr(lang, "demo_pace"), min_value=1.0, max_value=8.0, value=st.session_state.get("lam", 2.0), step=0.5, key="lam")
    refresh_interval = st.sidebar.slider(
        tr(lang, "refresh_every"), min_value=1.0, max_value=5.0, value=2.0, step=0.5, key="refresh_interval"
    )
    st.sidebar.caption(tr(lang, "pace_help"))

    st.sidebar.divider()
    st.sidebar.subheader(tr(lang, "show_scenario"))

    if st.sidebar.button(tr(lang, "risky_order"), use_container_width=True, type="primary"):
        inject_events(
            pipeline,
            [
                make_event(
                    action="checkout_completed",
                    amount=1450,
                    category="luxury",
                    user_id="demo_risk_customer",
                    location="Tokyo",
                    previous_location="Algiers",
                    seconds_since_location_change=18,
                )
            ],
        )
        st.sidebar.success(tr(lang, "scenario_added"))

    if st.sidebar.button(tr(lang, "demand_surge"), use_container_width=True):
        inject_events(
            pipeline,
            [make_event(action="product_view", category="fashion", user_id=f"surge_user_{i}", session_id=f"surge_{i}") for i in range(34)],
        )
        st.sidebar.success(tr(lang, "scenario_added"))

    if st.sidebar.button(tr(lang, "abandoned_cart"), use_container_width=True):
        inject_events(
            pipeline,
            [make_event(action="add_to_cart", category="electronics", user_id="demo_cart_customer", session_id="demo_abandoned_cart")]
        )
        cart = pipeline.session_carts.get("demo_abandoned_cart")
        if cart:
            aged_timestamp = datetime.now(timezone.utc) - timedelta(seconds=60)
            cart["first_seen"] = aged_timestamp
            cart["last_seen"] = aged_timestamp
        st.sidebar.success(tr(lang, "scenario_added"))

    if st.sidebar.button(tr(lang, "low_stock"), use_container_width=True):
        pipeline.stock_levels["luxury"] = 8
        st.sidebar.success(tr(lang, "scenario_added"))

    st.sidebar.divider()
    if st.sidebar.button(tr(lang, "reset_demo"), use_container_width=True):
        for key in ("pipeline", "generator"):
            st.session_state.pop(key, None)
        st.sidebar.success(tr(lang, "reset_done"))
        st.rerun()

    st.sidebar.divider()
    st.sidebar.subheader(tr(lang, "manual_event"))
    transactional_actions = {"checkout_completed", "checkout_failed", "card_velocity"}
    with st.sidebar.form("manual_event_form", clear_on_submit=False):
        action = st.selectbox(
            tr(lang, "business_event"),
            ["page_view", "product_view", "add_to_cart", "checkout_completed", "checkout_failed", "card_velocity"],
            key="manual_action",
        )
        category = st.selectbox(tr(lang, "category"), generator.categories, key="manual_category")
        amount = st.number_input(
            tr(lang, "order_value"),
            min_value=0.0,
            value=325.0,
            step=25.0,
            key="manual_amount",
            disabled=action not in transactional_actions,
            help=None if action in transactional_actions else tr(lang, "order_value_disabled_help"),
        )
        location = st.selectbox(tr(lang, "customer_location"), generator.cities, index=0, key="manual_location")
        impossible_travel = st.checkbox(tr(lang, "impossible_travel"), value=False, key="manual_impossible_travel")
        submitted = st.form_submit_button(tr(lang, "add_event"))

    if submitted:
        # Browsing events (page_view/product_view/add_to_cart) never carry a
        # monetary amount in the real event schema - keeping a stale
        # "order value" on them used to trigger a false amount_spike fraud
        # flag on ordinary traffic.
        effective_amount = amount if action in transactional_actions else 0.0
        inject_events(
            pipeline,
            [
                make_event(
                    action=action,
                    category=category,
                    amount=effective_amount,
                    location=location,
                    previous_location="Algiers" if impossible_travel else "",
                    seconds_since_location_change=12 if impossible_travel else None,
                    status="failed" if action == "checkout_failed" else "ok",
                )
            ],
        )
        st.sidebar.success(tr(lang, "event_added"))

    return running, lam, refresh_interval


def header(running: bool, lang: str) -> None:
    state = tr(lang, "live") if running else tr(lang, "paused")
    html = (
        '<div class="hero">'
        "<div>"
        f'<div class="eyebrow">{tr(lang, "eyebrow")}</div>'
        f'<div class="title">{tr(lang, "title")}</div>'
        f'<div class="subtitle">{tr(lang, "subtitle")}</div>'
        "</div>"
        f'<div class="state-pill">{state}</div>'
        "</div>"
    )
    st.markdown(html, unsafe_allow_html=True)


def top_metrics(metrics: dict[str, Any], lang: str) -> None:
    cols = st.columns(5)
    cols[0].metric(tr(lang, "sales"), euro(metrics["gmv"]), tr(lang, "approved_gmv"))
    cols[1].metric(tr(lang, "customers"), f"{metrics['active_users']:,}", tr(lang, "live_users"))
    cols[2].metric(tr(lang, "blocked_orders"), f"{metrics['blocked']:,}", pct(metrics["fraud_block_rate"]))
    cols[3].metric(tr(lang, "risk_level"), f"{metrics['avg_risk']:.2f}", tr(lang, "avg_score"))
    cols[4].metric(tr(lang, "service_health"), pct(1 - metrics["error_rate"]), f"{metrics['events_per_second']:.1f}/sec")


def business_note(title: str, body: str, style: str = "") -> None:
    st.markdown(
        f'<div class="business-note {style}"><strong>{title}</strong><span>{body}</span></div>',
        unsafe_allow_html=True,
    )


def action_box(tag: str, title: str, body: str, style: str = "") -> str:
    # NOTE: this must stay a single line with no leading whitespace. Markdown
    # (CommonMark) treats any line indented 4+ spaces as a preformatted code
    # block, so a "pretty" multi-line/indented f-string here gets rendered as
    # raw HTML text instead of a styled card.
    return (
        f'<div class="decision-card {style}">'
        f'<div class="tag">{tag}</div>'
        f"<strong>{title}</strong>"
        f"<span>{body}</span>"
        f"</div>"
    )


def build_recommendations(pipeline: StreamingPipeline, metrics: dict[str, Any], lang: str) -> list[dict[str, str]]:
    pricing = pipeline.dynamic_pricing()
    alerts = pipeline.inventory_alerts()
    offers = pipeline.abandoned_carts(age_seconds=45)
    health = pipeline.health_alarm()
    recs: list[dict[str, str]] = []

    if metrics["blocked"] > 0:
        recs.append(
            {
                "tag": tr(lang, "fraud_tag"),
                "title": tr(lang, "review_blocked"),
                "body": tr(lang, "review_blocked_detail", count=metrics["blocked"]),
                "style": "danger",
            }
        )
    else:
        recs.append(
            {
                "tag": tr(lang, "fraud_tag"),
                "title": tr(lang, "fraud_quiet"),
                "body": tr(lang, "fraud_quiet_detail"),
                "style": "success",
            }
        )

    if not pricing.empty and pricing.iloc[0].to_dict()["suggested_markup_pct"] > 0:
        top = pricing.iloc[0].to_dict()
        recs.append(
            {
                "tag": tr(lang, "revenue_tag"),
                "title": tr(lang, "raise_price", category=str(top["category"]).title()),
                "body": tr(lang, "raise_price_detail", views=int(top["views_60s"]), markup=int(top["suggested_markup_pct"])),
                "style": "warning",
            }
        )
    else:
        recs.append(
            {
                "tag": tr(lang, "revenue_tag"),
                "title": tr(lang, "no_markup"),
                "body": tr(lang, "no_markup_detail"),
                "style": "success",
            }
        )

    stock_alerts = [row for row in alerts if row["status"] == "stockout_alert"]
    if stock_alerts:
        first = stock_alerts[0]
        recs.append(
            {
                "tag": tr(lang, "inventory_tag"),
                "title": tr(lang, "restock", category=str(first["category"]).title()),
                "body": tr(lang, "restock_detail", stock=first["stock"]),
                "style": "warning",
            }
        )
    else:
        recs.append(
            {
                "tag": tr(lang, "inventory_tag"),
                "title": tr(lang, "inventory_stable"),
                "body": tr(lang, "inventory_stable_detail"),
                "style": "success",
            }
        )

    if offers:
        recs.append(
            {
                "tag": tr(lang, "retention_tag"),
                "title": tr(lang, "recover_carts"),
                "body": tr(lang, "recover_carts_detail", count=len(offers)),
                "style": "warning",
            }
        )
    else:
        recs.append(
            {
                "tag": tr(lang, "retention_tag"),
                "title": tr(lang, "no_carts"),
                "body": tr(lang, "no_carts_detail"),
                "style": "success",
            }
        )

    if health["alarm"]:
        body = tr(lang, "service_alarm_detail", rate=health["error_rate_pct"])
        title = tr(lang, "service_alarm")
        style = "danger"
    else:
        body = tr(lang, "service_ok_detail", rate=health["error_rate_pct"])
        title = tr(lang, "platform_healthy")
        style = "success"
    recs.append({"tag": tr(lang, "ops_tag"), "title": title, "body": body, "style": style})
    return recs


def decision_summary(pipeline: StreamingPipeline, metrics: dict[str, Any], lang: str) -> None:
    recommendations = build_recommendations(pipeline, metrics, lang)
    st.subheader(tr(lang, "exec_plan"))
    st.markdown(f'<div class="section-caption">{tr(lang, "exec_caption")}</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="decision-grid">' + "".join(action_box(item["tag"], item["title"], item["body"], item["style"]) for item in recommendations) + "</div>",
        unsafe_allow_html=True,
    )
    st.download_button(
        tr(lang, "download_decisions_csv"),
        data=pd.DataFrame(recommendations).to_csv(index=False).encode("utf-8"),
        file_name="action_plan.csv",
        mime="text/csv",
    )


def rename_columns(df, lang: str):
    return df.rename(columns=TEXT[lang]["columns"])


def fraud_tab(pipeline: StreamingPipeline, lang: str) -> None:
    st.subheader(tr(lang, "fraud_title"))
    st.markdown(f'<div class="section-caption">{tr(lang, "fraud_caption")}</div>', unsafe_allow_html=True)
    feed = pipeline.fraud_feed(12)
    risk_dist = pipeline.risk_distribution()
    blocked = pipeline.query_df(
        """
        SELECT ip_address, location, COUNT(*) AS blocked_orders, ROUND(MAX(risk_score), 2) AS worst_score
        FROM events_stream
        WHERE is_fraud
        GROUP BY ip_address, location
        ORDER BY blocked_orders DESC, worst_score DESC
        LIMIT 8
        """
    )

    if feed.empty:
        business_note(tr(lang, "no_urgent_fraud"), tr(lang, "no_urgent_fraud_detail"), "success")
    else:
        latest = feed.iloc[0].to_dict()
        business_note(
            tr(lang, "latest_risky"),
            tr(lang, "latest_risky_detail", score=latest["risk_score"], location=latest["location"], reasons=latest["risk_reasons"] or "behavior anomaly"),
            "danger" if latest["risk_score"] >= 0.70 else "warning",
        )

    left, right = st.columns([1.25, 1])
    with left:
        if not feed.empty:
            st.dataframe(rename_columns(feed, lang), use_container_width=True, hide_index=True)
            st.download_button(
                tr(lang, "download_fraud_csv"),
                data=feed.to_csv(index=False).encode("utf-8"),
                file_name="fraud_feed.csv",
                mime="text/csv",
            )
    with right:
        if not risk_dist.empty:
            st.bar_chart(risk_dist, x="risk_band", y="events", use_container_width=True)
        if not blocked.empty:
            st.dataframe(rename_columns(blocked, lang), use_container_width=True, hide_index=True)


def pricing_tab(pipeline: StreamingPipeline, lang: str) -> None:
    st.subheader(tr(lang, "pricing_title"))
    st.markdown(f'<div class="section-caption">{tr(lang, "pricing_caption")}</div>', unsafe_allow_html=True)
    pricing = pipeline.dynamic_pricing()
    if pricing.empty:
        business_note(tr(lang, "no_pricing"), tr(lang, "no_pricing_detail"), "success")
        return

    active = pricing[pricing["suggested_markup_pct"] > 0]
    if active.empty:
        business_note(tr(lang, "pricing_keep"), tr(lang, "pricing_keep_detail"), "success")
    else:
        for row in active.to_dict("records"):
            business_note(
                tr(lang, "increase_category", category=str(row["category"]).title(), markup=int(row["suggested_markup_pct"])),
                tr(lang, "increase_category_detail", views=int(row["views_60s"])),
                "warning",
            )

    st.bar_chart(pricing, x="category", y="views_60s", use_container_width=True)
    st.dataframe(rename_columns(pricing, lang), use_container_width=True, hide_index=True)


def recovery_tab(pipeline: StreamingPipeline, lang: str) -> None:
    st.subheader(tr(lang, "recovery_title"))
    st.markdown(f'<div class="section-caption">{tr(lang, "recovery_caption")}</div>', unsafe_allow_html=True)
    offers = pipeline.abandoned_carts(age_seconds=45)
    if not offers:
        business_note(tr(lang, "no_carts"), tr(lang, "no_carts_detail"), "success")
        return

    business_note(tr(lang, "send_offers"), tr(lang, "send_offers_detail", count=len(offers)), "warning")
    friendly = pd.DataFrame(offers)[["session_id", "user_id", "category", "age_seconds", "offer_payload"]]
    st.dataframe(rename_columns(friendly, lang), use_container_width=True, hide_index=True)


def inventory_tab(pipeline: StreamingPipeline, lang: str) -> None:
    st.subheader(tr(lang, "inventory_title"))
    st.markdown(f'<div class="section-caption">{tr(lang, "inventory_caption")}</div>', unsafe_allow_html=True)
    sales = pd.DataFrame(pipeline.category_sales())
    alerts = pd.DataFrame(pipeline.inventory_alerts())
    urgent = alerts[alerts["status"] == "stockout_alert"] if not alerts.empty else pd.DataFrame()

    if urgent.empty:
        business_note(tr(lang, "inventory_safe"), tr(lang, "inventory_safe_detail"), "success")
    else:
        for row in urgent.to_dict("records"):
            business_note(
                tr(lang, "reorder_category", category=str(row["category"]).title()),
                tr(lang, "reorder_category_detail", stock=row["stock"]),
                "warning",
            )

    left, right = st.columns([1.15, 1])
    with left:
        st.line_chart(sales, x="category", y=["stock", "sold"], use_container_width=True)
    with right:
        st.dataframe(rename_columns(alerts, lang), use_container_width=True, hide_index=True)


def health_tab(pipeline: StreamingPipeline, lang: str) -> None:
    st.subheader(tr(lang, "health_title"))
    st.markdown(f'<div class="section-caption">{tr(lang, "health_caption")}</div>', unsafe_allow_html=True)
    alarm = pipeline.health_alarm()
    title = tr(lang, "health_bad") if alarm["alarm"] else tr(lang, "health_good")
    body = tr(lang, "service_alarm_detail", rate=alarm["error_rate_pct"]) if alarm["alarm"] else tr(lang, "service_ok_detail", rate=alarm["error_rate_pct"])
    business_note(title, body, "danger" if alarm["alarm"] else "success")

    series = pipeline.ingestion_timeseries()
    if not series.empty:
        st.line_chart(series, x="second", y=["events", "failed_events"], use_container_width=True)
        with st.expander(tr(lang, "show_technical")):
            st.dataframe(series.tail(60), use_container_width=True, hide_index=True)


def main() -> None:
    lang = choose_language()
    inject_css(lang)
    pipeline = get_pipeline()
    generator = get_generator()
    running, lam, refresh_interval = sidebar_controls(pipeline, generator, lang)

    if running:
        simulate_tick(pipeline, generator, lam)

    metrics = pipeline.metrics()
    header(running, lang)
    top_metrics(metrics, lang)

    tab_names = tabs_for(lang)
    tab0, tab1, tab2, tab3, tab4, tab5 = st.tabs(tab_names)
    with tab0:
        decision_summary(pipeline, metrics, lang)
    with tab1:
        fraud_tab(pipeline, lang)
    with tab2:
        pricing_tab(pipeline, lang)
    with tab3:
        recovery_tab(pipeline, lang)
    with tab4:
        inventory_tab(pipeline, lang)
    with tab5:
        health_tab(pipeline, lang)

    st.caption(f"{tr(lang, 'footer')} {Path.cwd()}")

    if running:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
