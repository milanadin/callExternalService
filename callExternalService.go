// 1 Базовый пул воркеров
// Генератор (producer) кладёт `N` заданий (`Job{ID: 1..N}`) в канал `jobs`.
// `M` воркеров читают из `jobs`, обрабатывают через `callExternalService`, пишут `Result` в канал `results`.
// `main` собирает все результаты из `results` и печатает их.
// Завершиться корректно: дождаться всех воркеров и не уронить программу.

// 2 Потокобезопасная статистика + детектор гонок
// Добавить сборщик `Stats`: счётчики `panics` / `failed` и `map[workerID]int` (сколько заданий успешно сделал каждый воркер).
// Воркеры обновляют статистику конкурентно.

// 3 Ограничение параллелизма семафором
// Внешний сервис держит максимум `K` одновременных вызовов (например, `K=2` при `M=4` воркерах). Реализовать ограничение **семафором на буферизованном канале**.

// 4 Отмена через context + отсутствие утечек
// Протащить `context.Context` от `main` во все воркеры и в `callExternalService`.
// Завести общий таймаут операции через `context.WithTimeout` (например, 2–3 с).
// Воркеры обязаны **уважать** `ctx.Done()`: при отмене не начинать новую работу и не залипать.

// 5 Graceful shutdown
// Сервис должен корректно останавливаться по сигналу ОС (`SIGINT`/`SIGTERM`): перестать брать новые задания, дать текущим завершиться, но не висеть вечно.

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Job struct {
	ID int
}

type Result struct {
	ID  int
	Err error
}

type Stats struct {
	panics  atomic.Int32
	failed  atomic.Int32
	sucсess map[int]int
	mu      sync.RWMutex
}

func generator(ctx context.Context, N int) chan Job {
	jobs := make(chan Job)

	go func() {
		defer close(jobs)
		for i := 1; i <= N; i++ {
			select {
			case jobs <- Job{ID: i}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return jobs
}

func callCES(ctx context.Context, j Job, s *Stats, workerID int) (res Result) {

	defer func() {
		if r := recover(); r != nil {
			res = Result{Err: fmt.Errorf("%v", r)}
			s.panics.Add(1)
			return
		}
	}()

	r, err := callExternalService(ctx, j)
	if err != nil {
		res = Result{Err: err}
		s.failed.Add(1)
		return

	}

	res = Result{ID: r}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sucсess[workerID]++
	return
}

func workersPool(ctx context.Context, N int, M int, s *Stats, semaphore chan struct{}) chan Result {
	out := make(chan Result)

	jobs := generator(ctx, N)

	var wg sync.WaitGroup
	for i := 0; i < M; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case j, ok := <-jobs:
					if !ok {
						return
					}
					select {
					case semaphore <- struct{}{}:
					case <-ctx.Done():
						return
					}
					call := callCES(ctx, j, s, i)
					<-semaphore
					select {
					case out <- call:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func callExternalService(ctx context.Context, j Job) (int, error) {
	work := time.Duration(50+rand.Intn(250)) * time.Millisecond

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(work):
	}

	roll := rand.Intn(10)
	switch {
	case roll == 0:
		panic(fmt.Sprintf("сервис паникнул на job %d", j.ID))
	case roll <= 2:
		return 0, fmt.Errorf("сервис вернул ошибку на job %d", j.ID)
	default:
		return j.ID * j.ID, nil
	}
}

func gracefulShutdown(ctx context.Context, done chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		select {
		case <-done:
			log.Println("Все воркеры завершили работу. Сервис останавливается.")
		case <-shutdownCtx.Done():
			log.Println("Таймаут graceful shutdown завершен. Принудительная остановка.")
		}
	case <-done:
		return
	}
}

func printer(s *Stats, wg *sync.WaitGroup, res chan Result, done chan struct{}) {
	defer wg.Done()
	defer close(done)
	for r := range res {
		fmt.Println(r)
	}
	fmt.Println(s.failed.Load(), s.panics.Load())
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range s.sucсess {
		fmt.Println(k, v)
	}
}

func main() {
	const K = 4

	ctxParent, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctxParent, 3*time.Second)
	defer cancel()

	semaphore := make(chan struct{}, K)

	s := Stats{
		sucсess: make(map[int]int),
	}
	res := workersPool(ctx, 10, 3, &s, semaphore)

	done := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(1)
	go gracefulShutdown(ctx, done, &wg)

	wg.Add(1)

	go printer(&s, &wg, res, done)

	wg.Wait()
}
