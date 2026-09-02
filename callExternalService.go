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

package external

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

type Job struct {
	ID int
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
