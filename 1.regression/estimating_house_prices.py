'''
1.9 Let's apply all these principles to estimate housing prices.
Using "decision tree regressor with Adaptive Boosting" to solve this problem.
'''
import numpy as np
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import AdaBoostRegressor
from sklearn import datasets
from sklearn.metrics import mean_squared_error,explained_variance_score
from sklearn.utils import shuffle
import matplotlib.pyplot as plt

# housing dataset
housing_data = datasets.load_boston() # housing_data[data,target]

# separate dataset into trainging and testing
X,Y = shuffle(housing_data.data, housing_data.target, random_state=7)
num_training = int(0.8*len(X))
X_train, Y_train = X[:num_training],Y[:num_training]
X_test, Y_test = X[num_training:],Y[num_training:]


'''
compare fit results and see how AdaBoost really boosts the performance of decision tree regressor
'''
# fit a decision tree regression model
dt_regressor = DecisionTreeRegressor(max_depth=4)
dt_regressor.fit(X_train,Y_train)

# fit decision tree regression with AdaBoost
ab_regressor = AdaBoostRegressor(DecisionTreeRegressor(max_depth=4),n_estimators=400,random_state=7)
ab_regressor.fit(X_train,Y_train)

# evaluate the performance
y_pred_dt = dt_regressor.predict(X_test)
mse = mean_squared_error(Y_test,y_pred_dt)
evs = explained_variance_score(Y_test,y_pred_dt)
print("\n### Decision Tree performance ###")
print("\nMean squared error = ",round(mse,2))
print("\nExplained variance score = ",round(evs,2))

y_pred_ab = ab_regressor.predict(X_test)
mse = mean_squared_error(Y_test,y_pred_ab)
evs = explained_variance_score(Y_test,y_pred_ab)
print("\n### Decision Tree performance ###")
print("\nMean squared error = ",round(mse,2))
print("\nExplained variance score = ",round(evs,2))



'''
1.10 Computing the relative importance of features : scikit-learn functionality
'''

# define plot feature importances function
def plot_feature_importances(feature_importances,title,feature_names):
    # Normalize the importance values
    feature_importances = 100 * (feature_importances/max(feature_importances))
    
    # Sort the index values and flip them so that they are arranged in decreasing order of importance
    index_sorted = np.flipud(np.argsort(feature_importances))
    
    # Center the location of the labels on the X-axis
    pos = np.arange(index_sorted.shape[0])+0.5
    
    # Plot the bar graph
    plt.figure()
    plt.bar(pos, feature_importances[index_sorted], align='center')
    plt.xticks(pos, feature_names[index_sorted])
    plt.ylabel('Relative Importance')
    plt.title(title)
    plt.show()
    
# plot relative impoartance of each feature using regressor object : feature_importances_
plot_feature_importances(dt_regressor.feature_importances_,'Decision Tree Regressor', housing_data.feature_names)

plot_feature_importances(ab_regressor.feature_importances_,'AdaBoost Regressor',housing_data.feature_names)

