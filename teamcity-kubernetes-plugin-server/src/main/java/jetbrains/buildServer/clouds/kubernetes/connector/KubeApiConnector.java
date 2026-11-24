
package jetbrains.buildServer.clouds.kubernetes.connector;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import java.io.Closeable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;

/**
 * Created by ekoshkin (koshkinev@gmail.com) on 28.05.17.
 */
public interface KubeApiConnector extends Closeable {
    String NEVER_RESTART_POLICY = "Never";

    @NotNull
    KubeApiConnectionCheckResult testConnection();

    @NotNull
    Pod createPod(@NotNull Pod podTemplate);

    PersistentVolumeClaim createPVC(@NotNull PersistentVolumeClaim pvc);

    /**
     * Deletes the given pod.
     *
     * @param gracePeriod the pod's termination grace period in seconds, 0 to delete immediately
     * @return false if the pod does not exist in the cluster, i.e. it is already deleted; true if the
     * deletion was accepted by the cluster. Any failure other than "pod not found" is reported as a
     * {@link io.fabric8.kubernetes.client.KubernetesClientException} instead of returning false.
     * Callers must treat false as a successful deletion: absent pod means the goal of terminating the
     * instance is already reached.
     */
    boolean deletePod(@NotNull String podName, long gracePeriod);

    @NotNull
    Collection<Pod> listPods(@NotNull Map<String, String> labels);

    @Nullable
    Deployment getDeployment(@NotNull String deploymentName);

    String getNamespace();

    @Nullable
    PodStatus getPodStatus(@NotNull String podName);

    @NotNull
    Collection<String> listNamespaces();

    @NotNull
    Collection<String> listDeployments();

    /**
     * Deletes the named PVC.
     *
     * @return false if the PVC does not exist in the cluster, i.e. it is already deleted; true if the
     * deletion was accepted by the cluster. Any failure other than "PVC not found" is reported as a
     * {@link io.fabric8.kubernetes.client.KubernetesClientException} instead of returning false.
     */
    boolean deletePVC(@NotNull String name);

    void invalidate();
}