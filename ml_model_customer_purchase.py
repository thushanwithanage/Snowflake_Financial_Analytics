# Predicting Next-Month Customer Purchases Using XGBoost

from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, roc_auc_score
from xgboost import XGBClassifier
from config.read_data import get_data

df = get_data()

if df.empty:
    print("Data not found")
    exit(1)

# Data Cleaning

# Check for missing values
print(df.isnull().sum())

df["COUNTRY"] = df["COUNTRY"].fillna("Unknown")

# Create binary target
df["PURCHASE_NEXT_MONTH"] = (df["NEXT_MONTH_REVENUE"] > 0).astype(int)

# Encode categorical variables
cols = ["INDUSTRY", "COUNTRY"]
for col in cols:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col])

# Define Features & Target

X = df.drop(columns=["CUSTOMER_SK", "NEXT_MONTH_REVENUE", "PURCHASE_NEXT_MONTH"])
y = df["PURCHASE_NEXT_MONTH"]

# Train-Test Split

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Handle Class Imbalance

neg = len(y_train[y_train == 0])
pos = len(y_train[y_train == 1])

scale_pos_weight = neg / pos

# Train XGBoost Classifier

model = XGBClassifier(
    n_estimators=300,
    max_depth=4,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    eval_metric="logloss",
    scale_pos_weight=scale_pos_weight
)

model.fit(X_train, y_train)

# Predictions

y_prob = model.predict_proba(X_test)[:, 1]

threshold = 0.5
y_pred = (y_prob > threshold).astype(int)

# Evaluation

accuracy = accuracy_score(y_test, y_pred)
roc_auc = roc_auc_score(y_test, y_prob)

print("Accuracy:", accuracy)
print("ROC-AUC:", roc_auc)

print("\nClassification Report:\n")
print(classification_report(y_test, y_pred))

print("\nConfusion Matrix:\n")
print(confusion_matrix(y_test, y_pred))