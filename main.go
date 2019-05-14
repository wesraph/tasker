package tasker

import (
	"fmt"
)

var ErrStepNotFound = fmt.Errorf("Step not found")
var ErrMissingTaskName = fmt.Errorf("Missing task name")
var ErrMissingStepName = fmt.Errorf("Missing step name")
var ErrMissingSteps = fmt.Errorf("Missing steps in task")
var ErrMissingExecFunction = fmt.Errorf("Missing exec function")
var ErrReachedMaxRetry = fmt.Errorf("Reached max retry for task")
var ErrReachedEndOfTask = fmt.Errorf("Reached end of task")

type Step struct {
	Name string
	Exec func(t *Task) error
}

type Task struct {
	ID         string
	Name       string
	ActualStep string
	Status     string
	Steps      []Step
	Retry      int
	MaxRetry   int
	Buffer     map[string]string
	Args       map[string]string
}

func init() {
}

func (t *Task) Exec() error {
	err := t.initValidate()
	if err != nil {
		return err
	}

	actStep, err := t.getActualStep()
	for {
		err = actStep.Exec(t)
		if err != nil {
			if t.Retry+1 >= t.MaxRetry {
				return ErrReachedMaxRetry
			}

			t.Retry++
		} else {
			actStep, err = t.getNextStep()
			if err == ErrReachedEndOfTask {
				t.Status = "done"
				return nil
			} else if err != nil {
				return err
			}

			t.ActualStep = actStep.Name
		}
	}

}

func (t *Task) initValidate() error {
	if len(t.Steps) == 0 {
		return ErrMissingSteps
	}

	if t.Name == "" {
		return ErrMissingTaskName
	}

	for _, s := range t.Steps {
		if s.Name == "" {
			return ErrMissingTaskName
		}
		if s.Exec == nil {
			return ErrMissingExecFunction
		}
	}

	if t.ActualStep == "" {
		t.ActualStep = t.Steps[0].Name
	}

	if t.Status == "" {
		t.Status = "todo"
	}

	return nil
}

func (t *Task) getActualStep() (*Step, error) {
	if t.ActualStep == "" {
		return &t.Steps[0], nil
	}

	// Find the next step
	for _, s := range t.Steps {
		if s.Name == t.ActualStep {
			return &s, nil
		}
	}

	return nil, ErrStepNotFound
}

func (t *Task) getNextStep() (*Step, error) {
	if t.ActualStep == "" {
		return nil, ErrStepNotFound
	}

	// Find the next step
	for i, s := range t.Steps {
		if s.Name == t.ActualStep {
			if i+1 < len(t.Steps) {
				return &t.Steps[i+1], nil
			}

			// No next step, we finished the work
			return nil, ErrReachedEndOfTask
		}
	}

	return nil, ErrStepNotFound
}
