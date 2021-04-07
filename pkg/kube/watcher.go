package kube

import (
	"fmt"
	"strconv"

	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const annotationKey = "event-exporter.wandera.com/last-count"

type EventHandler func(event *EnhancedEvent)

type EventWatcher struct {
	informer        cache.SharedInformer
	stopper         chan struct{}
	labelCache      *LabelCache
	annotationCache *AnnotationCache
	fn              EventHandler
	clientset       *kubernetes.Clientset
}

func NewEventWatcher(config *rest.Config, namespace string, fn EventHandler) *EventWatcher {
	clientset := kubernetes.NewForConfigOrDie(config)
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0, informers.WithNamespace(namespace))
	informer := factory.Core().V1().Events().Informer()

	watcher := &EventWatcher{
		informer:        informer,
		stopper:         make(chan struct{}),
		labelCache:      NewLabelCache(config),
		annotationCache: NewAnnotationCache(config),
		fn:              fn,
		clientset:       clientset,
	}

	informer.AddEventHandler(watcher)

	return watcher
}

func (e *EventWatcher) OnAdd(obj interface{}) {
	event := obj.(*corev1.Event)
	e.onEvent(event)
}

func (e *EventWatcher) OnUpdate(oldObj, newObj interface{}) {
	event := newObj.(*corev1.Event)
	e.onEvent(event)
}

func (e *EventWatcher) onEvent(event *corev1.Event) {
	log.Debug().
		Str("msg", event.Message).
		Str("namespace", event.Namespace).
		Str("reason", event.Reason).
		Str("involvedObject", event.InvolvedObject.Name).
		Msg("Received event")

	if a, ok := event.Annotations[annotationKey]; ok {
		if v, err := strconv.Atoi(a); err == nil && v >= int(event.Count) {
			log.Debug().Msg("Skip event")
			return
		}
	}

	ev := &EnhancedEvent{
		Event: *event.DeepCopy(),
	}

	labels, err := e.labelCache.GetLabelsWithCache(&event.InvolvedObject)
	if err != nil {
		log.Error().Err(err).Msg("Cannot list labels of the object")
		// Ignoring error, but log it anyways
	} else {
		ev.InvolvedObject.Labels = labels
		ev.InvolvedObject.ObjectReference = *event.InvolvedObject.DeepCopy()
	}

	annotations, err := e.annotationCache.GetAnnotationsWithCache(&event.InvolvedObject)
	if err != nil {
		log.Error().Err(err).Msg("Cannot list annotations of the object")
	} else {
		ev.InvolvedObject.Annotations = annotations
		ev.InvolvedObject.ObjectReference = *event.InvolvedObject.DeepCopy()
	}

	e.fn(ev)

	if err = e.annotateEvent(event); err != nil {
		log.Error().Err(err).Msg("Cannot update annotations of the event")
	}

	return
}

func (e *EventWatcher) annotateEvent(event *corev1.Event) error {
	patchJson := []byte(fmt.Sprintf(`
		{
			"metadata": {
				"annotations": {
					"%s": "%d"
				}
			}
		}`, annotationKey, event.Count))
	_, err := e.clientset.CoreV1().Events(event.Namespace).PatchWithEventNamespace(event, patchJson)
	return err
}

func (e *EventWatcher) OnDelete(obj interface{}) {
	// Ignore deletes
}

func (e *EventWatcher) Start() {
	go e.informer.Run(e.stopper)
}

func (e *EventWatcher) Stop() {
	e.stopper <- struct{}{}
	close(e.stopper)
}
