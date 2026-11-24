
package jetbrains.buildServer.clouds.kubernetes.connector;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.base.BaseOperation;
import io.fabric8.kubernetes.client.dsl.base.OperationContext;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutionException;

public class FakeKubeClient extends DefaultKubernetesClient {

  public KubernetesClientException myException;

  /**
   * Name of the pod passed to the last delete request, and the grace period it was requested with.
   */
  public String myDeletedPodName;
  public long myDeletedPodGracePeriod;
  public boolean myDeletedPodGracePeriodWasSet;

  @Override
  public MixedOperation<Pod, PodList, PodResource<Pod>> pods() {
    if (myException != null){
      throw myException;
    }
    return new MyOperation(){
      @Override
      protected Object handleCreate(Object resource) {
        return new Pod();
      }

      @Override
      public PodResource<Pod> withName(String name) {
        return createRecordingPodResource(name);
      }
    };
  }

  /**
   * A dynamic proxy for the pod resource: PodResource has too many methods to implement by hand, and only
   * withGracePeriod and delete are relevant for the delete request.
   */
  private PodResource<Pod> createRecordingPodResource(String name) {
    return (PodResource<Pod>) Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[]{PodResource.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "delete":
                  myDeletedPodName = name;
                  return true;
                case "withGracePeriod":
                  myDeletedPodGracePeriod = (Long) args[0];
                  myDeletedPodGracePeriodWasSet = true;
                  return proxy;
                default:
                  throw new UnsupportedOperationException(method.getName() + " is not used by the delete request");
              }
            });
  }

  private static class MyOperation extends BaseOperation{

    protected MyOperation() {
      super(new OperationContext(null, null, null, null, null, null, null, false, null, null, null, null, null, null,
              null, null, false, 10000, null, false, false, null));
    }
  }
}