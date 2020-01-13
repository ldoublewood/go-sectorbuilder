package sectorbuilder

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"

	"golang.org/x/xerrors"
)

type WorkerTaskType int

const (
	WorkerPreCommit WorkerTaskType = iota
	WorkerCommit
)

type WorkerTask struct {
	Type   WorkerTaskType
	TaskID uint64

	SectorID uint64

	// preCommit
	SealTicket SealTicket
	Pieces     []PublicPieceInfo

	// commit
	SealSeed SealSeed
	Rspco    RawSealPreCommitOutput
}

type workerCall struct {
	task      WorkerTask
	ret       chan SealRes
	workerDir string
}

func (sb *SectorBuilder) AddWorker(ctx context.Context, cfg WorkerCfg) (<-chan WorkerTask, error) {
	sb.remoteLk.Lock()
	defer sb.remoteLk.Unlock()

	workerDir := filepath.Join(sb.filesystem.path, "workers", cfg.IPAddress)

	umountCmd := exec.CommandContext(ctx, "umount", "-f", workerDir)
	umountRes, err := umountCmd.Output()
	log.Infof("Executed umount worker directory: %s, err: %s", umountRes, err)

	err = os.MkdirAll(workerDir, 0777)
	if err != nil {
		return nil, err
	}
	// You should install `sshfs`
	mountCmd := exec.CommandContext(ctx, "sshfs", cfg.IPAddress+":"+cfg.Directory, workerDir)
	mountRes, err := mountCmd.Output()
	if err != nil {
		log.Errorf("Executed sshfs mount worker directory: %s, err: %s", mountRes, err)
		return nil, err
	}

	taskCh := make(chan WorkerTask)
	r := &remote{
		sealTasks: taskCh,
		busy:      0,
		dir:       workerDir,
	}

	sb.remoteCtr++
	sb.remotes[sb.remoteCtr] = r

	go sb.remoteWorker(ctx, r, cfg)

	return taskCh, nil
}

func (sb *SectorBuilder) returnTask(task workerCall) {
	var ret chan workerCall
	switch task.task.Type {
	case WorkerPreCommit:
		ret = sb.precommitTasks
	case WorkerCommit:
		log.Error("discard committing task", task.task.SectorID)
	default:
		log.Error("unknown task type", task.task.Type)
	}

	go func() {
		select {
		case ret <- task:
		case <-sb.stopping:
			return
		}
	}()
}

func (sb *SectorBuilder) remoteWorker(ctx context.Context, r *remote, cfg WorkerCfg) {
	defer log.Warn("Remote worker disconnected")

	defer func() {
		sb.remoteLk.Lock()
		defer sb.remoteLk.Unlock()

		for i, vr := range sb.remotes {
			if vr == r {
				delete(sb.remotes, i)
				return
			}
		}
	}()

	getTask := func() chan workerCall {
		if r.lastPreCommitSectorID > 0 {
			return r.commitTask
		} else {
			return sb.precommitTasks
		}
	}

	for {
		select {
		case task := <-getTask():
			sb.doTask(ctx, r, task)
		case <-ctx.Done():
			return
		case <-sb.stopping:
			return
		}

		r.lk.Lock()
		r.busy = 0
		r.lk.Unlock()
	}
}

func (sb *SectorBuilder) doTask(ctx context.Context, r *remote, task workerCall) {
	resCh := make(chan SealRes)

	sb.remoteLk.Lock()
	sb.remoteResults[task.task.TaskID] = resCh
	sb.remoteLk.Unlock()

	// send the task
	select {
	case r.sealTasks <- task.task:
	case <-ctx.Done():
		sb.returnTask(task)
		return
	}

	r.lk.Lock()
	r.busy = task.task.TaskID
	r.lk.Unlock()

	// wait for the result
	select {
	case res := <-resCh:

		// send the result back to the caller
		select {
		case task.ret <- res:
			sb.remoteLk.Lock()
			if task.task.Type == WorkerPreCommit {
				r.lastPreCommitSectorID = task.task.SectorID
				r.commitTask = make(chan workerCall)
			} else {
				r.lastPreCommitSectorID = 0
				r.commitTask = nil
			}
			sb.remoteLk.Unlock()
		case <-ctx.Done():
			return
		case <-sb.stopping:
			return
		}

	case <-ctx.Done():
		log.Warnf("context expired while waiting for task %d (sector %d): %s", task.task.TaskID, task.task.SectorID, ctx.Err())
		return
	case <-sb.stopping:
		return
	}
}

func (sb *SectorBuilder) TaskDone(ctx context.Context, task uint64, res SealRes) error {
	sb.remoteLk.Lock()
	rres, ok := sb.remoteResults[task]
	if ok {
		delete(sb.remoteResults, task)
	}
	sb.remoteLk.Unlock()

	if !ok {
		return xerrors.Errorf("task %d not found", task)
	}

	select {
	case rres <- res:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sb *SectorBuilder) putCommitTask(call *workerCall) chan workerCall {
	sb.remoteLk.Lock()
	defer sb.remoteLk.Unlock()

	for _, r := range sb.remotes {
		if r.lastPreCommitSectorID == call.task.SectorID {
			call.workerDir = r.dir
			return r.commitTask
		}
	}
	return make(chan workerCall) // will never succeed to send to
}
