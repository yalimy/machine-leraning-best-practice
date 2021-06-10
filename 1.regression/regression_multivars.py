import sys
import numpy as np

filename = sys.argv[1]
X=[]
Y=[]
with open(filename,'r') as f:
    for line in f.readlines():
        data = [float(i) for i in line.split(',')]
        xt,yt = data[:-1],data[-1]
        X.append(xt)
        Y.append(yt)
        
# train/test split
num_training = int(0.8*len(X))
num_test  = len(X) - num_training

# training data
# X_train = np.array(X[:num_training]).reshape((num_training,1))
X_train = np.array(X[:num_training])
Y_train = np.array(Y[:num_training])

# testing data
# X_test = np.array(X[num_training:]).reshape((num_test,1))
X_test = np.array(X[num_training:])
Y_test = np.array(Y[num_training:])


# create linear regression object
from sklearn import linear_model

linear_regressor = linear_model.LinearRegression()
ridge_regressor = linear_model.Ridge(alpha=0.01,fit_intercept=True,max_iter=10000)

# train the model using training sets
linear_regressor.fit(X_train,Y_train)
ridge_regressor.fit(X_train,Y_train)

# Predict output
y_test_pred = linear_regressor.predict(X_test)
y_test_pred_ridge = ridge_regressor.predict(X_test)

# Measure perfromance
import sklearn.metrics as sm

print("\nLinear")
print("\nMean absolute error =",round(sm.mean_absolute_error(Y_test,y_test_pred),2))
print("\nMean squared error =",round(sm.mean_squared_error(Y_test,y_test_pred),2))
print("\nMedian absolute error =",round(sm.median_absolute_error(Y_test,y_test_pred),2))
print("\nExplained variance score =",round(sm.explained_variance_score(Y_test,y_test_pred),2))
print("\nR2 score =",round(sm.r2_score(Y_test,y_test_pred),2))

print("\nRidge")
print("\nMean absolute error =",round(sm.mean_absolute_error(Y_test,y_test_pred_ridge),2))
print("\nMean squared error =",round(sm.mean_squared_error(Y_test,y_test_pred_ridge),2))
print("\nMedian absolute error =",round(sm.median_absolute_error(Y_test,y_test_pred_ridge),2))
print("\nExplained variance score =",round(sm.explained_variance_score(Y_test,y_test_pred_ridge),2))
print("\nR2 score =",round(sm.r2_score(Y_test,y_test_pred_ridge),2))


# Polynomial regression
from sklearn.preprocessing import PolynomialFeatures
import matplotlib.pyplot as plt

polynomial = PolynomialFeatures(degree=10)

X_train_transformed = polynomial.fit_transform(X_train) #用多项式的系数表示数据点
X_test_transformed = polynomial.fit_transform(X_test)
datapoint = [0.39,2.78,7.11]
poly_datapoint = polynomial.fit_transform(np.array(datapoint).reshape(1,-1))

poly_linear_model = linear_model.LinearRegression()
poly_linear_model.fit(X_train_transformed,Y_train)

print("\nLinear Regression :",linear_regressor.predict(np.array(datapoint).reshape(1,-1)))
print("\nPolynomial Regression :",poly_linear_model.predict(poly_datapoint))

''' 如何实现多维数据的可视化？
y_train_pred_poly = poly_linear_model.predict(X_train_transformed)
y_test_pred_poly = poly_linear_model.predict(X_test_transformed)
plt.figure()
plt.scatter(X_train,Y_train,'*')
plt.scatter(X_test,Y_test,'*')
plt.plot(X_train_transformed,y_train_pred_poly,'red',linewidth=4)
plt.plot(X_test_transformed,y_test_pred_poly,'blue',linewidth=4)
plot.show()
'''