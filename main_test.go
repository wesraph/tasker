package tasker

import (
	"fmt"
	"testing"
)

func testStep(t *Task) error {
	return nil
}

func testFailingStep(t *Task) error {
	return fmt.Errorf("test failing task")
}

func testStep2(t *Task) error {
	return nil
}

func testRenameNextStep(t *Task) error {
	t.ActualStep = "doesnt_exists"
	return nil
}

func TestExec1(t *testing.T) {
	task := &Task{
		Name:       "test",
		ActualStep: "step1",
		Steps: []Step{
			{
				Name: "step1",
				Exec: testStep,
			},
			{
				Name: "step2",
				Exec: testStep2,
			},
		},
	}

	err := task.Exec()
	if err != nil {
		t.Errorf("Error while testing task execution")
	}
}

func TestExec2(t *testing.T) {
	task := &Task{
		Name: "test",
		Steps: []Step{
			{
				Name: "step1",
				Exec: testStep,
			},
			{
				Name: "step2",
				Exec: testStep2,
			},
		},
	}

	err := task.Exec()
	if err != nil {
		t.Errorf("Error while testing task execution")
	}
}

func TestExec3(t *testing.T) {
	task := &Task{
		Steps: []Step{
			{
				Name: "step1",
				Exec: testStep,
			},
			{
				Name: "step2",
				Exec: testStep2,
			},
		},
	}

	err := task.Exec()
	if err != ErrMissingTaskName {
		t.Errorf("Error while testing task execution")
	}
}

func TestRetry(t *testing.T) {
	task := &Task{
		Name:       "test",
		ActualStep: "step1",
		Steps: []Step{
			{
				Name: "step1",
				Exec: testStep,
			},
			{
				Name: "step2",
				Exec: testFailingStep,
			},
		},
	}

	err := task.Exec()
	if err == nil {
		t.Errorf("Should fail because of retry")
	}
}

func UnnamedTask(t *testing.T) {
	task := &Task{
		Name:       "test",
		ActualStep: "step1",
		Steps: []Step{
			{
				Name: "step1",
				Exec: testStep,
			},
			{
				Name: "step2",
				Exec: testFailingStep,
			},
		},
	}

	err := task.Exec()
	if err == nil {
		t.Errorf("Should fail because of retry")
	}
}

func RenameTask(t *testing.T) {
	task := &Task{
		Name:       "test",
		ActualStep: "step1",
		Steps: []Step{
			{
				Name: "step1",
				Exec: testRenameNextStep,
			},
			{
				Name: "step2",
				Exec: testStep,
			},
		},
	}

	err := task.Exec()
	if err == nil {
		t.Errorf("Should fail because of retry")
	}
}
