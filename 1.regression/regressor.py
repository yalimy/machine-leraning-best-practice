import sys
import numpy as np
from sklearn import linear_model    # 回归模型
import matplotlib.pyplot as plt     # 可视化
import sklearn.metrics as sm        # 预测评估

# prepare data for y=f(x)
filename = sys.argv[1]
X = []
Y = []

with open(filename,'r') as f:
    for line in f.readlines():
        xt,yt = [float(i) for i in line.split(',')]
        X.append(xt)
        Y.append(yt)
        
# seperate dataset to training and testing
num_training = int(0.8 * len(X))
num_test = len(X) - num_training

# training dataset
X_train = np.array(X[:num_training]).reshape((num_training,1))
Y_train = np.array(Y[:num_training])

# testing dataset
X_test = np.array(X[num_training:]).reshape((num_test,1))
Y_test = np.array(Y[num_training:])

''' 1.1
# training the model : linear regressor
# create linear regressor object
linear_regressor = linear_model.LinearRegression()
linear_regressor.fit(X_train,Y_train)

# 可视化训练数据
y_train_pred = linear_regressor.predict(X_train)
plt.figure()
plt.scatter(X_train,Y_train,color='green')
plt.plot(X_train,y_train_pred,color='red',linewidth=4)

plt.title("Training Data")
plt.show()

# 可视化测试数据
y_test_pred = linear_regressor.predict(X_test)
plt.scatter(X_test,Y_test,color='green')
plt.plot(X_test,y_test_pred,color='yellow',linewidth=4)
plt.title('Test data')
plt.show()


# 预测评估：平均误差，平均方差，误差中值，解释性方差评分，R2评分
# 一个好的模型表现：平均方差低，而解释性方差评分高
print("\nMean absolute error =",round(sm.mean_absolute_error(Y_test,y_test_pred),2))
print("\nMean squared error =",round(sm.mean_squared_error(Y_test,y_test_pred),2))
print("\nMedian absolute error =",round(sm.median_absolute_error(Y_test,y_test_pred),2))
print("\nExplained variance score =",round(sm.explained_variance_score(Y_test,y_test_pred),2))
print("\nR2 score =",round(sm.r2_score(Y_test,y_test_pred),2))
'''

''' 1.2
# 模型持久化归档
import pickle
achive_model_file = 'saved_model.pkl'
with open('saved_model.pkl','wb') as f:
    pickle.dump(linear_regressor,f)
    
with open('saved_model.pkl','rb') as f:
    model_linear=pickle.load(f)
    
y_test_pred_new = model_linear.predict(X_test)
print("\nNew mean absolute error =",round(sm.mean_absolute_error(Y_test,y_test_pred_new),2))
'''

''' 1.3
# Ridge Regressor
ridge_regressor = linear_model.Ridge(alpha=0.01,fit_intercept=True,max_iter=10000)
ridge_regressor.fit(X_train,Y_train)
y_test_pred_ridge = ridge_regressor.predict(X_test)

plt.figure()
plt.scatter(X_test,Y_test,color='black')
plt.plot(X_test,y_test_pred_ridge,color='red',linewidth=4)
plt.title("Ridge Regressor")
plt.show()

print("\nMean absolute error =",round(sm.mean_absolute_error(Y_test,y_test_pred_ridge),2))
print("\nMean squared error =",round(sm.mean_squared_error(Y_test,y_test_pred_ridge),2))
print("\nMedian absolute error =",round(sm.median_absolute_error(Y_test,y_test_pred_ridge),2))
print("\nExplained variance score =",round(sm.explained_variance_score(Y_test,y_test_pred_ridge),2))
print("\nR2 score =",round(sm.r2_score(Y_test,y_test_pred_ridge),2))
'''