import numpy
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.datasets import load_iris
from sklearn.datasets import load_boston
import warnings


warnings.filterwarnings('ignore')


def test_numpy():
    pass


def test_reg():
    boston = load_boston()
    X, Y = boston.data, boston.target
    # clf = LinearRegression()
    clf = LogisticRegression()
    clf.fit(X, Y)
    err = 0
    for x, y in zip(X, Y):
        yhat = clf.predict(x)
        if yhat[0] != y:
            err += 1
            print("%d %d" % (yhat[0], y))
    print("error rate: %f" % (err*1.0/len(X)))


def test_ensemble():
    iris = load_iris()
    trainX = iris.data
    trainY = iris.target
    rfc = RandomForestClassifier()
    rfc.fit(trainX, trainY)
    err = 0
    for x, y in zip(trainX, trainY):
        yhat = rfc.predict(x)
        if yhat[0] != y:
            err += 1
            print("error %d %d" % (yhat[0], y))
    print("error rate: %f" % (err*1.0/len(trainX)))


if __name__ == '__main__':
    test_numpy()
    #test_ensemble()
    test_reg()
