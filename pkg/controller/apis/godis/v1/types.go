package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type Godis struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GodisSpec   `json:"spec"`
	Status GodisStatus `json:"status"`
}

type GodisSpec struct {
	Replicas *int32 `json:"replicas"`
}

type GodisStatus struct {
	Replicas int32  `json:"replicas"`
	Members  string `json:"members"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type GodisList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `son:"metadata,omitempty"`

	Items []*Godis `json:"items"`
}
