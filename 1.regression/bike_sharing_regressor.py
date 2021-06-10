'''
使用随机森林回归模型进行预测，随机森林是一组决策树，并利用决策树组平均值提升性能
'''
import csv
import numpy as np
import sys
from sklearn.ensemble import RandomForestRegressor
from estimating_house_prices import plot_feature_importances

# 1. processing datasets
def load_dataset(filename):
    file_reader = csv.reader(open(filename,'r'),delimiter=',')
    X,Y = [], []
    for row in file_reader:
        X.append(row[2:13])
        Y.append(row[-1])
        
     # Extract feature names
     feature_names = np.array(X[0])
        
     # Remove the first row because they are feature names
     return np.array(X[1:]).astype(np.float32), np.array(Y[1:]).astype(np.float32), feature_names

X, Y ,feature_names = load_dataset(sys.argv[1])
X, Y = shuffle(X,Y,random_state=7)

# 2. separate training, test datasets
num_training = int(0.9*len(X))
x_train, y_train = X[:num_training], Y[:num_training]
x_test, y_test = X[num_training:], Y[num_training:]

# 3. training "Random Forest Regressor" model,一定要注意 min_samples_split 参数值
rf_regressor = RandomForestRegressor(n_estimators=1000, max_depth=10, min_samples_split=0.01)
rf_regressor.fit(x_train,y_train)

# 4. evaluate model performance
y_pred_rf = rf_regressor.predict(x_test)
mse = mean_squared_error(y_test,y_pred_rf)
evs = explained_variance_score(y_test,y_pred_rf)

print("\nRandom Forest Regressor Performance")
print("\nMean squared error = ",round(mse,2))
print("\nExplained variance score =",round(evs,2))

# 5. feature importance relative plot
plot_feature_importances(rf_regressor.feature_importances_,'Random Forest Regressor',feature_names)