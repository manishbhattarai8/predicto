import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import PolynomialFeatures
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

# Load the CSV
data = pd.read_csv("nepse_index_last365.csv")
print(data.head())
prices = data["Close"].values

# Prepare dataset with window=2 for demo (use larger windows with more data)
window_size = 3
X, y = [], []
for i in range(len(prices) - window_size):
    X.append(prices[i:i + window_size])
    y.append(prices[i + window_size])

X, y = np.array(X), np.array(y)

# Train/Test split
split = int(0.8 * len(X)) or len(X) - 1  # Use all but last sample for training
X_train, X_test = X[:split], X[split:]
y_train, y_test = y[:split], y[split:]

# Polynomial feature transformation (degree=2 or 3 usually works)
poly = PolynomialFeatures(degree=2)   # you can try degree=3 also
X_train_poly = poly.fit_transform(X_train)
X_test_poly = poly.transform(X_test)


# Train model
model = LinearRegression()
model.fit(X_train_poly, y_train) #finding the best weights

# Test & predict
preds = model.predict(X_test_poly)
rmse = np.sqrt(mean_squared_error(y_test, preds))

# Predict tomorrow
tomorrow_input = prices[-window_size:].reshape(1, -1) #comverts to 2D array, 1 row and detect colums automatically 
tomorrow_pred = model.predict(poly.transform(tomorrow_input))[0] #that 0 turns it to scalar from array

# Create next day's date (assuming consecutive days)
# Ensure dates are datetime objects
data["Date"] = pd.to_datetime(data["Date"])

# Create next day's date (assuming consecutive days)
next_date = data["Date"].iloc[-1] + pd.Timedelta(days=1)

# Plot
plt.figure(figsize=(12, 6))

# Plot actual prices
plt.plot(data["Date"], data["Close"], linestyle="-", color="blue", label="Actual")

# Highlight predicted next day
plt.scatter(next_date, tomorrow_pred, color="red", marker="x", s=100, label="Predicted")
plt.text(next_date, tomorrow_pred, f"{tomorrow_pred:.2f}", fontsize=10, color="red", ha="left", va="bottom")

# Formatting x-axis for large date range
plt.gca().xaxis.set_major_locator(mdates.MonthLocator())  # show ticks for each month
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))  # format as 'Jan 2025'

plt.xticks(rotation=45)  # rotate labels
plt.title("NEPSE Index - Last 365 Days + Predicted Next Day (Polynomial Regression)")
plt.xlabel("Date")
plt.ylabel("Closing Price")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

print("RMSE:", rmse)
print("Predicted Next Day Price:", tomorrow_pred)

