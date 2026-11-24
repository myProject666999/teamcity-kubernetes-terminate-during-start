
package jetbrains.buildServer.clouds.kubernetes;

import io.fabric8.kubernetes.api.model.Pod;
import jetbrains.buildServer.clouds.CloudErrorInfo;
import jetbrains.buildServer.clouds.CloudInstance;
import jetbrains.buildServer.clouds.InstanceStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Created by ekoshkin (koshkinev@gmail.com) on 30.05.17.
 */
public interface KubeCloudInstance extends CloudInstance {
    @NotNull
    @Override
    KubeCloudImage getImage();

    void setStatus(final InstanceStatus status);

    void updateState(Pod pod);

    void setError(@Nullable CloudErrorInfo errorInfo);

    @Nullable
    String getPVCName();

    /**
     * Records that termination of this instance was requested. Set by the thread calling
     * {@code KubeCloudClient.terminateInstance} before the termination task is submitted, so it is
     * already visible to the creation task of the same instance, which may run after the termination task.
     */
    void requestTermination();

    /**
     * @return true if {@link #requestTermination()} has already been called for this instance
     */
    boolean isTerminationRequested();

    /**
     * Must be called exactly once by the asynchronous creation task, when it is done with the cluster
     * regardless of the outcome. Atomically marks the creation as finished and decides who deletes the
     * Pod and the PVC, so that exactly one of the creation and the termination task does it.
     *
     * @return true if the creation task must delete the Pod and the PVC it has created, because a
     * termination was requested while the creation task was running. Returns false if the termination
     * task owns the cleanup (already ran or will run the deletion itself), or if no termination was
     * requested at all.
     */
    boolean completeCreation();

    /**
     * Called by the termination task to decide whether it deletes the Pod and the PVC itself.
     *
     * @return true if the termination task must delete the resources. Returns false when the creation
     * task has not finished yet: that task will delete the resources itself, because the termination
     * request flag is already set by the time it calls {@link #completeCreation()}.
     */
    boolean claimCleanupForTermination();
}