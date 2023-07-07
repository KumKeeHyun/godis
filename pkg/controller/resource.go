package controller

import (
	"fmt"
	"strconv"
	"strings"
)

func configMapName(clusterName string) string {
	return clusterName + "-config"
}

func godisName(clusterName string, id int) string {
	return fmt.Sprintf("%s-%d", clusterName, id)
}

func splitGodisNameID(godisName string) (clusterName string, id int, err error) {
	parts := strings.Split(godisName, "-")
	if len(parts) < 2 {
		return "", 0, fmt.Errorf("unexpected godisName format: %s", godisName)
	}
	id, err = strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return "", 0, fmt.Errorf("unexpected godisName format: %s", godisName)
	}
	return strings.Join(parts[:len(parts)-1], "-"), id, nil
}

func serviceName(clusterName string, id int) string {
	return fmt.Sprintf("%s-%d-endpoint", clusterName, id)
}

func serviceNameWithGodisName(godisName string) string {
	return godisName + "-endpoint"
}

func serviceFQDN(godisNamespace, godisName string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", serviceNameWithGodisName(godisName), godisNamespace)
}

func godisPeerURL(godisNamespace, godisName string) string {
	return fmt.Sprintf("http://%s:6300", serviceFQDN(godisNamespace, godisName))
}

func volumeClaimName(godisName string) string {
	return "data-" + godisName
}
