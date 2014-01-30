package eu.europeana.cloud.test;

import java.util.ArrayList;
import java.util.List;

import junitparams.internal.ParameterisedTestClassRunner;
import junitparams.internal.TestMethod;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

public class CassandraParamertizedTestRunner extends CassandraTestRunner {

    private ParameterisedTestClassRunner parameterisedRunner = new ParameterisedTestClassRunner(getTestClass());


    public CassandraParamertizedTestRunner(Class<?> c)
            throws InitializationError {
        super(c);
    }


    @Deprecated
    protected void validateInstanceMethods(List<Throwable> errors) {
        validatePublicVoidNoArgMethods(After.class, false, errors);
        validatePublicVoidNoArgMethods(Before.class, false, errors);
    }


    @Override
    protected List<FrameworkMethod> computeTestMethods() {
        return parameterisedRunner.computeFrameworkMethods();
    }


    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        if (handleIgnored(method, notifier))
            return;

        if (method.getAnnotation(Ignore.class) != null) {
            Description ignoredMethod = parameterisedRunner.describeParameterisedMethod(method);
            for (Description child : ignoredMethod.getChildren()) {
                notifier.fireTestIgnored(child);
            }
            return;
        }

        TestMethod testMethod = parameterisedRunner.testMethodFor(method);
        if (parameterisedRunner.shouldRun(testMethod)) {
            parameterisedRunner.runParameterisedTest(testMethod, methodBlock(method), notifier);
        } else {
            super.runChild(method, notifier);
        }
    }


    private boolean handleIgnored(FrameworkMethod method, RunNotifier notifier) {
        TestMethod testMethod = parameterisedRunner.testMethodFor(method);
        if (testMethod.isIgnored())
            notifier.fireTestIgnored(describeMethod(method));

        return testMethod.isIgnored();
    }


    @Override
    public Description getDescription() {
        Description description = Description.createSuiteDescription(getName(), getTestClass().getAnnotations());

        List<FrameworkMethod> resultMethods = new ArrayList<FrameworkMethod>();
        resultMethods.addAll(parameterisedRunner.returnListOfMethods());

        for (FrameworkMethod method : resultMethods)
            description.addChild(describeMethod(method));

        return description;
    }


    private Description describeMethod(FrameworkMethod method) {
        Description child = parameterisedRunner.describeParameterisedMethod(method);

        if (child == null)
            child = describeChild(method);

        return child;
    }


    @Override
    protected Statement methodInvoker(FrameworkMethod method, Object test) {
        Statement methodInvoker = parameterisedRunner.parameterisedMethodInvoker(method, test);
        if (methodInvoker == null)
            methodInvoker = super.methodInvoker(method, test);

        return methodInvoker;
    }

}
