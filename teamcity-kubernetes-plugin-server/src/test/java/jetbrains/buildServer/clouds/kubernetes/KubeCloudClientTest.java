
package jetbrains.buildServer.clouds.kubernetes;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.util.concurrent.*;
import jetbrains.buildServer.BaseTestCase;
import jetbrains.buildServer.clouds.CloudClientParameters;
import jetbrains.buildServer.clouds.CloudErrorInfo;
import jetbrains.buildServer.clouds.CloudException;
import jetbrains.buildServer.clouds.CloudImage;
import jetbrains.buildServer.clouds.CloudImageParameters;
import jetbrains.buildServer.clouds.CloudInstance;
import jetbrains.buildServer.clouds.CloudInstanceUserData;
import jetbrains.buildServer.clouds.InstanceStatus;
import jetbrains.buildServer.clouds.kubernetes.connector.FakeKubeApiConnector;
import jetbrains.buildServer.clouds.kubernetes.connector.ImagePullPolicy;
import jetbrains.buildServer.clouds.kubernetes.connector.KubeApiConnector;
import jetbrains.buildServer.clouds.kubernetes.podSpec.BuildAgentPodTemplateProvider;
import jetbrains.buildServer.clouds.kubernetes.podSpec.BuildAgentPodTemplateProviders;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static jetbrains.buildServer.clouds.kubernetes.KubeParametersConstants.PROFILE_INSTANCE_LIMIT;
import static org.assertj.core.api.BDDAssertions.then;

/**
 * Created by ekoshkin (koshkinev@gmail.com) on 29.06.17.
 */
@Test
public class KubeCloudClientTest extends BaseTestCase {
    private static final String POD_SPEC_MODE = "DUMMY_SPEC_MODE";
    private static final AtomicInteger myMockIndex = new AtomicInteger();

    private Mockery m;
    private KubeApiConnector myApi;
    private BuildAgentPodTemplateProviders myPodTemplateProviders;
    private Map<String, BuildAgentPodTemplateProvider> myPodTemplateProvidersMap;
    private ExecutorService myExecutorService;

    private KubeBackgroundUpdater myUpdater = new KubeBackgroundUpdater() {
        @Override
        public void registerClient(@NotNull KubeCloudClient client) {

        }

        @Override
        public void unregisterClient(@NotNull KubeCloudClient client) {

        }
    };

    @BeforeMethod
    public void setUp() throws Exception {
        super.setUp();
        m = new Mockery();
        myApi = m.mock(KubeApiConnector.class);
        myExecutorService = new InProcessExecutor();
        myPodTemplateProvidersMap = new HashMap<>();
        myPodTemplateProviders = new BuildAgentPodTemplateProviders() {
            @NotNull
            @Override
            public Collection<BuildAgentPodTemplateProvider> getAll() {
                return myPodTemplateProviders.getAll();
            }

            @Nullable
            @Override
            public BuildAgentPodTemplateProvider find(@Nullable String id) {
                return myPodTemplateProvidersMap.get(id);
            }
        };
    }

    @AfterMethod
    public void tearDown() throws Exception {
        m.assertIsSatisfied();
        super.tearDown();
    }

    public void testIsInitialized() throws Exception {
        assertTrue(createClient(Collections.emptyList()).isInitialized());
    }

    public void testCanStartNewInstance_UnknownImage() throws Exception {
        KubeCloudClient cloudClient = createClient(Collections.emptyList());
        CloudImage image = m.mock(KubeCloudImage.class);
        m.checking(new Expectations(){{
            allowing(image).getId(); will(returnValue("image-1-id"));
        }});
        assertFalse(cloudClient.canStartNewInstance(image));
    }

    public void testCanStartNewInstance() throws Exception {
        KubeCloudImage image = m.mock(KubeCloudImage.class);
        m.checking(new Expectations(){{
            allowing(image).getName(); will(returnValue("image-1-name"));
            allowing(image).getId(); will(returnValue("image-1-id"));
            allowing(image).getRunningInstanceCount(); will(returnValue(0));
            allowing(image).getInstanceLimit(); will(returnValue(0));
        }});
        List<KubeCloudImage> images = Collections.singletonList(image);
        KubeCloudClient cloudClient = createClient(images);
        assertFalse(cloudClient.canStartNewInstance(image));
    }

    @Test
    public void testCanStartNewInstance2() throws Exception {
        KubeCloudImage image = m.mock(KubeCloudImage.class);
        m.checking(new Expectations(){{
            allowing(image).getName(); will(returnValue("image-1-name"));
            allowing(image).getId(); will(returnValue("image-1-id"));
            allowing(image).getRunningInstanceCount(); will(returnValue(1));
            allowing(image).getInstanceLimit(); will(returnValue(5));
        }});
        List<KubeCloudImage> images = Collections.singletonList(image);
        KubeCloudClient cloudClient = createClient(images);
        assertTrue(cloudClient.canStartNewInstanceWithDetails(image).isPositive());
    }

    public void testCanStartNewInstance_ProfileLimit() throws Exception {
        KubeCloudImage image = m.mock(KubeCloudImage.class);
        m.checking(new Expectations(){{
            allowing(image).getId(); will(returnValue("image-1-id"));
            allowing(image).getRunningInstanceCount(); will(returnValue(1));
            allowing(image).getInstanceLimit(); will(returnValue(2));
        }});
        List<KubeCloudImage> images = Collections.singletonList(image);
        CloudClientParameters cloudClientParameters = new MockCloudClientParameters(Collections.singletonMap(PROFILE_INSTANCE_LIMIT, "1"));
        KubeCloudClient cloudClient = createClient(images, cloudClientParameters);
        assertFalse(cloudClient.canStartNewInstance(image));
    }

    public void testCanStartNewInstance_ProfileLimit2() throws Exception {
        KubeCloudImage image = m.mock(KubeCloudImage.class);
        m.checking(new Expectations(){{
            allowing(image).getId(); will(returnValue("image-1-id"));
            allowing(image).getRunningInstanceCount(); will(returnValue(3));
            allowing(image).getInstanceLimit(); will(returnValue(5));
        }});
        List<KubeCloudImage> images = Collections.singletonList(image);
        CloudClientParameters cloudClientParameters = new MockCloudClientParameters(Collections.singletonMap(PROFILE_INSTANCE_LIMIT, "5"));
        KubeCloudClient cloudClient = createClient(images, cloudClientParameters);
        assertTrue(cloudClient.canStartNewInstanceWithDetails(image).isPositive());
    }

    public void testCanStartNewInstance_ImageLimit() throws Exception {
        KubeCloudImage image = m.mock(KubeCloudImage.class);
        m.checking(new Expectations(){{
            allowing(image).getName(); will(returnValue("image-1-name"));
            allowing(image).getId(); will(returnValue("image-1-id"));
            allowing(image).getRunningInstanceCount(); will(returnValue(1));
            allowing(image).getInstanceLimit(); will(returnValue(1));
        }});
        List<KubeCloudImage> images = Collections.singletonList(image);
        KubeCloudClient cloudClient = createClient(images);
        assertFalse(cloudClient.canStartNewInstance(image));
    }

    public void testDuplicateImageName() throws Exception {
        KubeCloudImage image1 = m.mock(KubeCloudImage.class, "1");
        KubeCloudImage image2 = m.mock(KubeCloudImage.class, "2");
        m.checking(new Expectations(){{
            allowing(image1).getId(); will(returnValue("image-1-id"));
            allowing(image1).getName(); will(returnValue("image"));
            allowing(image1).getRunningInstanceCount(); will(returnValue(0));
            allowing(image2).getId(); will(returnValue("image-2-id"));
            allowing(image2).getName(); will(returnValue("image"));
            allowing(image2).getRunningInstanceCount(); will(returnValue(0));
        }});
        createClient(Arrays.asList(image1, image2));
    }

    public void start_new_instance_failure_pvc() throws InterruptedException {
        final String podSpecMode = "DUMMY_SPEC_MODE";
        final CloudInstanceUserData userData = createInstanceTag();
        final BuildAgentPodTemplateProvider provider = m.mock(BuildAgentPodTemplateProvider.class);
        KubeCloudImage image = m.mock(KubeCloudImage.class);
        KubeCloudClientParametersImpl parameters = KubeCloudClientParametersImpl.create(new MockCloudClientParameters(Collections.emptyMap()));
        Pod podTemplate = new Pod();
        ObjectMeta podMetadata = new ObjectMeta();
        podMetadata.setName("image-1-id-123");
        podTemplate.setMetadata(podMetadata);
        podMetadata.setLabels(new HashMap<>());

        PersistentVolumeClaim pvcTemplate = new PersistentVolumeClaim();
        PersistentVolumeClaim newPVC = new PersistentVolumeClaim();
        {
            ObjectMeta pvcMetadata = new ObjectMeta();
            pvcMetadata.setName("image-1-id-123");
            pvcTemplate.setMetadata(pvcMetadata);
        }
        {
            ObjectMeta pvcMetadata = new ObjectMeta();
            pvcMetadata.setName("image-1-id-123");
            newPVC.setMetadata(pvcMetadata);
        }

        KubernetesClientException podCreationException = new KubernetesClientException("Some error on pod creation");

        myPodTemplateProvidersMap.put(podSpecMode, provider);

        m.checking(new Expectations(){{
            allowing(image).getName(); will(returnValue("image-1-name"));
            allowing(image).getId(); will(returnValue("image-1-id"));
            allowing(image).getPodSpecMode(); will(returnValue(podSpecMode));
            allowing(image).addStartedInstance(with(any(KubeCloudInstance.class)));
            allowing(myApi).getNamespace(); will(returnValue("leer"));
            oneOf(provider).getPodTemplate("image-1-id-123", userData, image, myApi); will(returnValue(podTemplate));
            oneOf(provider).getPVC("image-1-id-123", image); will(returnValue(pvcTemplate));
            oneOf(myApi).createPVC(pvcTemplate); will(returnValue(newPVC));
            oneOf(myApi).createPod(podTemplate); will(throwException(podCreationException));
            oneOf(myApi).deletePVC("image-1-id-123"); // should delete PVC if can't create pod
        }});
        List<KubeCloudImage> images = Collections.singletonList(image);
        KubeCloudClient cloudClient = createClient("defaultServerUuid", "defaultProfileId", images, parameters);
        try {
            cloudClient.startNewInstance(image, userData);
            fail("An exception " + podCreationException + " should have been rethrown");
        } catch (Exception ex) {
            then(ex).isEqualTo(podCreationException);
        }
        m.assertIsSatisfied();
    }

    /**
     * Reproduces the race this change fixes: the termination task runs before the creation task on the
     * two-thread executor. Previously deletePod returned false for the not-yet-existing pod, the
     * termination threw after three attempts and the instance was left STOPPING while the pod,
     * created afterwards, stayed in the cluster forever.
     */
    public void terminate_runs_before_creation_cleans_up_created_pod() throws Exception {
        final Fixture f = new Fixture("instance-1", true);
        final KubeCloudClient cloudClient = f.createClient();

        final KubeCloudInstance instance = (KubeCloudInstance) cloudClient.startNewInstance(f.image(), createInstanceTag());
        then(f.myExecutor.pendingTaskCount()).isEqualTo(1); // creation task is not executed yet

        cloudClient.terminateInstance(instance);
        then(f.myExecutor.pendingTaskCount()).isEqualTo(2);

        // termination task first, creation task after it
        f.myExecutor.runTask(1);
        f.myExecutor.runTask(0);

        then(f.myExecutor.getTaskFailures()).isEmpty();

        final List<String> calls = f.myApi.getCalls();
        final int createPodIndex = calls.indexOf("createPod:instance-1");
        then(createPodIndex).isGreaterThanOrEqualTo(0);
        then(calls.subList(createPodIndex + 1, calls.size()))
                .containsExactly("deletePod:instance-1", "deletePVC:instance-1");
        then(f.myApi.myExistingPods).isEmpty();
        then(f.myApi.myExistingPVCs).isEmpty();

        then(instance.getStatus()).isEqualTo(InstanceStatus.STOPPED);
        then(instance.getErrorInfo()).isNull();
    }

    public void terminate_runs_before_creation_without_pvc_cleans_up_created_pod() throws Exception {
        final Fixture f = new Fixture("instance-1", false);
        final KubeCloudClient cloudClient = f.createClient();

        final KubeCloudInstance instance = (KubeCloudInstance) cloudClient.startNewInstance(f.image(), createInstanceTag());
        cloudClient.terminateInstance(instance);

        f.myExecutor.runTask(1);
        f.myExecutor.runTask(0);

        then(f.myExecutor.getTaskFailures()).isEmpty();
        then(f.myApi.getCalls()).containsExactly("createPod:instance-1", "deletePod:instance-1");
        then(instance.getStatus()).isEqualTo(InstanceStatus.STOPPED);
    }

    /**
     * The reverse order: the creation task has already completed, so the termination task performs the
     * deletion itself. Behaviour of this path is unchanged apart from treating false as "already deleted".
     */
    public void creation_runs_before_termination_deletes_pod_and_pvc() throws Exception {
        final Fixture f = new Fixture("instance-1", true);
        final KubeCloudClient cloudClient = f.createClient();

        final KubeCloudInstance instance = (KubeCloudInstance) cloudClient.startNewInstance(f.image(), createInstanceTag());
        f.myExecutor.runTask(0);
        then(instance.getStatus()).isEqualTo(InstanceStatus.SCHEDULED_TO_START);

        cloudClient.terminateInstance(instance);
        f.myExecutor.runTask(1);

        then(f.myExecutor.getTaskFailures()).isEmpty();
        then(f.myApi.getCalls()).containsExactly(
                "createPVC:instance-1", "createPod:instance-1", "deletePod:instance-1", "deletePVC:instance-1");
        then(instance.getStatus()).isEqualTo(InstanceStatus.STOPPED);
    }

    /**
     * Creation failed and a termination was requested while the creation task was running: the PVC is
     * deleted by the creation task, there is no pod to delete, and the instance must not be left in ERROR.
     */
    public void creation_failure_with_pending_termination_ends_stopped() throws Exception {
        final Fixture f = new Fixture("instance-1", true);
        final KubernetesClientException failure = new KubernetesClientException("pod creation failed");
        f.myApi.failPodCreationWith(failure);
        final KubeCloudClient cloudClient = f.createClient();

        final KubeCloudInstance instance = (KubeCloudInstance) cloudClient.startNewInstance(f.image(), createInstanceTag());
        cloudClient.terminateInstance(instance);

        f.myExecutor.runTask(1);
        f.myExecutor.runTask(0);

        then(instance.getStatus()).isEqualTo(InstanceStatus.STOPPED);
        then(instance.getErrorInfo()).isNull();
        // createPVC, failed createPod, PVC cleanup by the creation task; nothing else to delete
        then(f.myApi.getCalls()).containsExactly("createPVC:instance-1", "createPod:instance-1", "deletePVC:instance-1");
        then(f.myApi.myExistingPVCs).isEmpty();
        then(f.myExecutor.getTaskFailures()).hasSize(1);
        then(f.myExecutor.getTaskFailures().get(0)).isEqualTo(failure);
    }

    /**
     * Creation failed without any termination request: unchanged behaviour, the instance ends in ERROR.
     */
    public void creation_failure_without_termination_ends_error() throws Exception {
        final Fixture f = new Fixture("instance-1", true);
        final KubernetesClientException failure = new KubernetesClientException("pod creation failed");
        f.myApi.failPodCreationWith(failure);
        final KubeCloudClient cloudClient = f.createClient();

        final KubeCloudInstance instance = (KubeCloudInstance) cloudClient.startNewInstance(f.image(), createInstanceTag());
        f.myExecutor.runTask(0);

        then(instance.getStatus()).isEqualTo(InstanceStatus.ERROR);
        then(instance.getErrorInfo()).isNotNull();
        then(f.myApi.getCalls()).containsExactly("createPVC:instance-1", "createPod:instance-1", "deletePVC:instance-1");
        then(f.myExecutor.getTaskFailures()).hasSize(1);
    }

    /**
     * A deletion failure which is not a KubernetesClientException (e.g. blocked credentials reported as a
     * CloudException) must end up as an error state, not as an exception swallowed by the executor.
     */
    public void termination_deletion_failure_is_reported_on_instance() throws Exception {
        final Fixture f = new Fixture("instance-1", true);
        f.myApi.failPodDeletionWith(new CloudException("credentials are blocked"));
        final KubeCloudClient cloudClient = f.createClient();

        final KubeCloudInstance instance = (KubeCloudInstance) cloudClient.startNewInstance(f.image(), createInstanceTag());
        f.myExecutor.runTask(0);

        cloudClient.terminateInstance(instance);
        f.myExecutor.runTask(1);

        then(f.myExecutor.getTaskFailures()).isEmpty();
        then(instance.getStatus()).isEqualTo(InstanceStatus.ERROR);
        then(instance.getErrorInfo()).isNotNull();
    }

    /**
     * Termination of an instance whose pod is already gone must still end STOPPED: false returned by
     * deletePod means "already absent", not "failed".
     */
    public void termination_with_absent_pod_ends_stopped() throws Exception {
        final Fixture f = new Fixture("instance-1", true);
        final KubeCloudClient cloudClient = f.createClient();

        final KubeCloudInstance instance = (KubeCloudInstance) cloudClient.startNewInstance(f.image(), createInstanceTag());
        f.myExecutor.runTask(0);
        // the pod disappeared without the plugin knowing, e.g. it was deleted externally
        f.myApi.myExistingPods.clear();
        f.myApi.myExistingPVCs.clear();

        cloudClient.terminateInstance(instance);
        f.myExecutor.runTask(1);

        then(f.myExecutor.getTaskFailures()).isEmpty();
        then(instance.getStatus()).isEqualTo(InstanceStatus.STOPPED);
        then(instance.getErrorInfo()).isNull();
    }

    /**
     * Termination of an instance created from an already existing pod (no creation task is running for it).
     */
    public void termination_of_instance_without_creation_task_ends_stopped() throws Exception {
        final Fixture f = new Fixture("instance-1", true);
        final KubeCloudClient cloudClient = f.createClient();
        final Pod pod = f.myProvider.getPodTemplate("instance-1", createInstanceTag(), f.image(), f.myApi);
        KubeTeamCityLabels.addCustomLabel(pod, KubeTeamCityLabels.POD_PVC_NAME, "instance-1");
        final KubeCloudInstanceImpl instance = new KubeCloudInstanceImpl(f.image(), pod);
        f.myApi.myExistingPods.add(instance.getName());
        f.myApi.myExistingPVCs.add(instance.getPVCName());

        cloudClient.terminateInstance(instance);
        f.myExecutor.runTask(0);

        then(f.myExecutor.getTaskFailures()).isEmpty();
        then(f.myApi.getCalls()).containsExactly("deletePod:instance-1", "deletePVC:instance-1");
        then(instance.getStatus()).isEqualTo(InstanceStatus.STOPPED);
    }

    /**
     * Runs the two tasks on real threads with random interleavings: whatever the order, the pod and the
     * PVC created for the instance must be deleted exactly once and the instance must end STOPPED.
     */
    public void creation_and_termination_race_always_cleans_up() throws Exception {
        for (int iteration = 0; iteration < 200; iteration++) {
            final Fixture f = new Fixture("instance-" + iteration, true);
            final CountingImage image = f.synchronisedImage();
            final KubeCloudClient cloudClient = f.createClient(image);
            final KubeCloudInstance instance = (KubeCloudInstance) cloudClient.startNewInstance(image, createInstanceTag());
            cloudClient.terminateInstance(instance);

            final CountDownLatch start = new CountDownLatch(1);
            final ExecutorService threads = Executors.newFixedThreadPool(2);
            final Future<?> creation = threads.submit(() -> {
                await(start);
                f.myExecutor.runTask(0);
            });
            final Future<?> termination = threads.submit(() -> {
                await(start);
                f.myExecutor.runTask(1);
            });
            start.countDown();
            creation.get();
            termination.get();
            threads.shutdown();

            then(f.myExecutor.getTaskFailures()).isEmpty();
            then(f.myApi.myExistingPods).isEmpty();
            then(f.myApi.myExistingPVCs).isEmpty();
            then(instance.getStatus()).isEqualTo(InstanceStatus.STOPPED);
            then(f.myApi.getCalls()).contains("createPod:" + f.myInstanceName);
            then(f.myApi.getCalls()).contains("deletePod:" + f.myInstanceName);
            then(f.myApi.getCalls()).contains("deletePVC:" + f.myInstanceName);
        }
    }

    private static void await(@NotNull CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private class Fixture {
        /**
         * Created on demand: the ordering tests use the Mockery for the image, the concurrency test uses
         * a real image object and must not create hundreds of identically named mocks.
         */
        @Nullable
        private KubeCloudImage myImage;

        private final RecordingKubeApiConnector myApi = new RecordingKubeApiConnector();
        private final ManualExecutor myExecutor = new ManualExecutor();
        private final String myInstanceName;
        private final boolean pvcRequired;
        private final BuildAgentPodTemplateProvider myProvider = new BuildAgentPodTemplateProvider() {
            @NotNull
            @Override
            public String getId() {
                return POD_SPEC_MODE;
            }

            @NotNull
            @Override
            public String getDisplayName() {
                return POD_SPEC_MODE;
            }

            @Nullable
            @Override
            public String getDescription() {
                return null;
            }

            @NotNull
            @Override
            public Pod getPodTemplate(@NotNull String instanceName,
                                      @NotNull CloudInstanceUserData cloudInstanceUserData,
                                      @NotNull KubeCloudImage kubeCloudImage,
                                      @NotNull KubeApiConnector apiConnector) {
                final Pod pod = new Pod();
                final ObjectMeta metadata = new ObjectMeta();
                metadata.setName(myInstanceName);
                metadata.setLabels(new HashMap<>());
                pod.setMetadata(metadata);
                return pod;
            }

            @Nullable
            @Override
            public PersistentVolumeClaim getPVC(@NotNull String instanceName, @NotNull KubeCloudImage kubeCloudImage) {
                if (!pvcRequired) return null;
                final PersistentVolumeClaim pvc = new PersistentVolumeClaim();
                final ObjectMeta metadata = new ObjectMeta();
                metadata.setName(myInstanceName);
                pvc.setMetadata(metadata);
                return pvc;
            }
        };

        Fixture(@NotNull String instanceName, boolean pvcRequired) {
            myInstanceName = instanceName;
            this.pvcRequired = pvcRequired;
            myPodTemplateProvidersMap.put(POD_SPEC_MODE, myProvider);
        }

        @NotNull
        KubeCloudImage image() {
            if (myImage == null) {
                myImage = m.mock(KubeCloudImage.class, "image-" + myMockIndex.incrementAndGet());
            }
            return myImage;
        }

        KubeCloudClient createClient() {
            return createClient(image());
        }

        /**
         * @return an image backed by a real object rather than the Mockery, for tests which run the
         * tasks on several threads: the Mockery is single threaded.
         */
        CountingImage synchronisedImage() {
            return new CountingImage(myInstanceName);
        }

        KubeCloudClient createClient(@NotNull KubeCloudImage image) {
            if (image == myImage) {
                m.checking(new Expectations(){{
                    allowing(myImage).getName(); will(returnValue("image-1-name"));
                    allowing(myImage).getId(); will(returnValue("image-1-id"));
                    allowing(myImage).getPodSpecMode(); will(returnValue(POD_SPEC_MODE));
                    allowing(myImage).addStartedInstance(with(any(KubeCloudInstance.class)));
                    allowing(myImage).getRunningInstanceCount(); will(returnValue(1));
                    allowing(myImage).getInstanceLimit(); will(returnValue(-1));
                    allowing(myImage).getAgentName(with(any(String.class))); will(returnValue("agent-name"));
                    allowing(myImage).populateInstances();
                }});
            }
            final KubeCloudClientParametersImpl parameters =
                    KubeCloudClientParametersImpl.create(new MockCloudClientParameters(Collections.emptyMap()));
            return new KubeCloudClient(myApi, "defaultServerUuid", "defaultProfileId", Collections.singletonList(image),
                                       parameters, myUpdater, myPodTemplateProviders, myExecutor,
                                       ignored -> myInstanceName);
        }
    }

    private static class CountingImage implements KubeCloudImage {
        private final String myId;

        CountingImage(@NotNull String id) {
            myId = id;
        }

        @Override
        public void populateInstances() {
        }

        @Override
        public void addStartedInstance(@NotNull KubeCloudInstance instance) {
        }

        @NotNull
        @Override
        public String getPodSpecMode() {
            return POD_SPEC_MODE;
        }

        @Nullable
        @Override
        public String getPodTemplate() {
            return null;
        }

        @Nullable
        @Override
        public String getPVCTemplate() {
            return null;
        }

        @Nullable
        @Override
        public String getSourceDeploymentName() {
            return null;
        }

        @Override
        public int getRunningInstanceCount() {
            return 1;
        }

        @Override
        public int getInstanceLimit() {
            return -1;
        }

        @NotNull
        @Override
        public String getAgentName(@NotNull String instanceName) {
            return instanceName;
        }

        @NotNull
        @Override
        public String getAgentNamePrefix() {
            return "";
        }

        @Override
        public void setErrorInfo(@Nullable CloudErrorInfo errorInfo) {
        }

        @Override
        public boolean isReusingNames() {
            return false;
        }

        @Nullable
        @Override
        public String getDockerImage() {
            return null;
        }

        @Nullable
        @Override
        public ImagePullPolicy getImagePullPolicy() {
            return null;
        }

        @Nullable
        @Override
        public String getDockerArguments() {
            return null;
        }

        @Nullable
        @Override
        public String getDockerCommand() {
            return null;
        }

        @NotNull
        @Override
        public String getId() {
            return myId;
        }

        @NotNull
        @Override
        public String getName() {
            return myId;
        }

        @NotNull
        @Override
        public Collection<? extends CloudInstance> getInstances() {
            return Collections.emptyList();
        }

        @Nullable
        @Override
        public CloudInstance findInstanceById(@NotNull String id) {
            return null;
        }

        @Nullable
        @Override
        public Integer getAgentPoolId() {
            return null;
        }

        @Nullable
        @Override
        public CloudErrorInfo getErrorInfo() {
            return null;
        }
    }

    @NotNull
    private KubeCloudClient createClient(List<KubeCloudImage> images) {
        return createClient("defaultServerUuid", "defaultProfileId", images, new MockCloudClientParameters(Collections.emptyMap()));
    }

    @NotNull
    private KubeCloudClient createClient(List<KubeCloudImage> images, CloudClientParameters cloudClientParameters) {
        return createClient("defaultServerUuid", "defaultProfileId", images, cloudClientParameters);
    }

    @NotNull
    private KubeCloudClient createClient(String serverUuid, String profileId, List<KubeCloudImage> images, CloudClientParameters cloudClientParameters) {
        return createClient(serverUuid, profileId, images, new KubeCloudClientParametersImpl(cloudClientParameters));
    }
    @NotNull
    private KubeCloudClient createClient(String serverUuid, String profileId, List<KubeCloudImage> images, KubeCloudClientParameters parameters) {
        return new KubeCloudClient(myApi, serverUuid, profileId, images, parameters, myUpdater,
                                   myPodTemplateProviders, myExecutorService, image -> String.format("%s-123", image.getId()));
    }

    private CloudInstanceUserData createInstanceTag() {
        return new CloudInstanceUserData("agent name", "auth token", "server address", null, "profile id", "profile description", Collections.emptyMap());
    }

    private static class InProcessExecutor extends AbstractExecutorService {
        @Override
        public void shutdown() {
        }

        @Override
        @NotNull
        public List<Runnable> shutdownNow() {
            return null;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public void execute(@NotNull final Runnable command) {
            command.run();
        }

        @NotNull
        @Override
        public Future<?> submit(@NotNull Runnable task) {
            execute(task);
            return new CompletableFuture<>();
        }
    }

    /**
     * Executor which never runs submitted tasks by itself. Tests decide explicitly which task runs and
     * when, so the order of the asynchronous creation and termination tasks of the same instance is
     * reproducible without sleeping.
     */
    private static class ManualExecutor extends AbstractExecutorService {
        private final List<Runnable> myTasks = new ArrayList<>();
        private final List<Throwable> myTaskFailures = new ArrayList<>();

        @Override
        public void execute(@NotNull final Runnable command) {
            myTasks.add(command);
        }

        @NotNull
        @Override
        public Future<?> submit(@NotNull Runnable task) {
            execute(task);
            return new CompletableFuture<>();
        }

        int pendingTaskCount() {
            return myTasks.size();
        }

        /**
         * Runs the task submitted at the given position, catching and recording any exception it leaks.
         */
        void runTask(int index) {
            final Runnable task = myTasks.get(index);
            try {
                task.run();
            } catch (Throwable t) {
                myTaskFailures.add(t);
            }
        }

        List<Throwable> getTaskFailures() {
            return Collections.unmodifiableList(myTaskFailures);
        }

        @Override
        public void shutdown() {
        }

        @Override
        @NotNull
        public List<Runnable> shutdownNow() {
            final List<Runnable> remaining = new ArrayList<>(myTasks);
            myTasks.clear();
            return remaining;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(final long timeout, final TimeUnit unit) {
            return true;
        }
    }

    /**
     * Records the order of the connector calls and treats every absent object as already deleted,
     * like a connector which is tolerant to concurrent cleanup.
     */
    private static class RecordingKubeApiConnector extends FakeKubeApiConnector {
        private final List<String> myCalls = new ArrayList<>();
        private final List<String> myExistingPods = new ArrayList<>();
        private final List<String> myExistingPVCs = new ArrayList<>();
        @Nullable
        private KubernetesClientException myCreatePodFailure;
        @Nullable
        private RuntimeException myDeletePodFailure;

        List<String> getCalls() {
            return Collections.unmodifiableList(myCalls);
        }

        void failPodCreationWith(@Nullable KubernetesClientException failure) {
            myCreatePodFailure = failure;
        }

        void failPodDeletionWith(@Nullable RuntimeException failure) {
            myDeletePodFailure = failure;
        }

        @NotNull
        @Override
        public Pod createPod(@NotNull final Pod podTemplate) {
            final String name = podTemplate.getMetadata().getName();
            myCalls.add("createPod:" + name);
            if (myCreatePodFailure != null) throw myCreatePodFailure;
            myExistingPods.add(name);
            return podTemplate;
        }

        @Override
        public PersistentVolumeClaim createPVC(@NotNull final PersistentVolumeClaim pvc) {
            final String name = pvc.getMetadata().getName();
            myCalls.add("createPVC:" + name);
            myExistingPVCs.add(name);
            return pvc;
        }

        @Override
        public boolean deletePod(@NotNull final String podName, final long gracePeriod) {
            myCalls.add("deletePod:" + podName);
            if (myDeletePodFailure != null) throw myDeletePodFailure;
            return myExistingPods.remove(podName);
        }

        @Override
        public boolean deletePVC(@NotNull final String name) {
            myCalls.add("deletePVC:" + name);
            return myExistingPVCs.remove(name);
        }
    }


    private class MockCloudClientParameters extends CloudClientParameters {
        private final Map<String, String> myParameters;

        public MockCloudClientParameters(Map<String, String> parameters) {
            myParameters = parameters;
        }

        @Nullable
        @Override
        public String getParameter(@NotNull String s) {
            return myParameters.get(s);
        }

        @NotNull
        @Override
        public Collection<String> listParameterNames() {
            return myParameters.keySet();
        }

        @NotNull
        @Override
        public Collection<CloudImageParameters> getCloudImages() {
            return null;
        }

        @NotNull
        @Override
        public Map<String, String> getParameters() {
            return Collections.unmodifiableMap(myParameters);
        }

        @NotNull
        @Override
        public String getProfileId() {
            return null;
        }

        @NotNull
        @Override
        public String getProfileDescription() {
            return null;
        }
    }
}