package controller

import (
	"context"
	"github.com/KumKeeHyun/godis/pkg/controller"
	clientset "github.com/KumKeeHyun/godis/pkg/controller/generated/clientset/versioned"

	godisinformers "github.com/KumKeeHyun/godis/pkg/controller/generated/informers/externalversions"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"os/signal"
	"syscall"
	"time"
)

const (
	keyMasterURL      = "masterurl"
	keyKubeConfigPath = "kubeconfigpath"
)

func New(vp *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use: "controller",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runController(vp)
		},
	}

	flags := cmd.Flags()
	flags.String(keyMasterURL, "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	flags.String(keyKubeConfigPath, "", "Path to a kubeconfigpath. Only required if out-of-cluster.")
	vp.BindPFlags(flags)

	return cmd
}

func runController(vp *viper.Viper) error {
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	logger := klog.FromContext(ctx)

	cfg, err := clientcmd.BuildConfigFromFlags(vp.GetString(keyMasterURL), vp.GetString(keyKubeConfigPath))
	if err != nil {
		logger.Error(err, "Error building kubeconfig")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		logger.Error(err, "Error building kubernetes clientset")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	godisClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		logger.Error(err, "Error building kubernetes clientset")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	godisInformerFactory := godisinformers.NewSharedInformerFactory(godisClient, time.Second*30)

	godisController := controller.New(ctx, kubeClient, godisClient,
		godisInformerFactory.Kumkeehyun().V1().GodisClusters(),
		godisInformerFactory.Kumkeehyun().V1().Godises(),
		controller.NewClient(),
	)

	// notice that there is no need to run Start methods in a separate goroutine. (i.e. go kubeInformerFactory.Start(ctx.done())
	// Start method is non-blocking and runs all registered informers in a dedicated goroutine.
	kubeInformerFactory.Start(ctx.Done())
	godisInformerFactory.Start(ctx.Done())

	if err = godisController.Run(ctx, 1); err != nil {
		logger.Error(err, "Error running controller")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		return err
	}
	return nil
}
