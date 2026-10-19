# parralelVdz4

Машина: i7-2630QM (4 ядра / 8 потоков), Windows 10, JDK 21.
Параметры: 10 потоков, 200 задач, ~10 секунд работы на прогон, прогрев 500 задач перед замерами.
Числа: submit / execution в миллисекундах.

Режим SLEEP (задачи со Thread.sleep):

UNIFORM
ThreadPerTask: submit 27, execution 124
FixedThreadPool: submit 1.9, execution 1063
RoundRobin: submit 0.3, execution 1154
WorkStealing: submit 0.4, execution 1113

PERIODIC
ThreadPerTask: submit 30, execution 532
FixedThreadPool: submit 1.9, execution 1030
RoundRobin: submit 0.2, execution 10125
WorkStealing: submit 0.3, execution 1004

PARETO
ThreadPerTask: submit 26, execution 3244
FixedThreadPool: submit 1.6, execution 3865
RoundRobin: submit 0.2, execution 3737
WorkStealing: submit 0.2, execution 3733

Режим BLACK_HOLE (sleep заменён на blackHole):

UNIFORM
ThreadPerTask: submit 32, execution 2681
FixedThreadPool: submit 1.6, execution 2814
RoundRobin: submit 0.4, execution 3181
WorkStealing: submit 0.1, execution 2736

PERIODIC
ThreadPerTask: submit 30, execution 3275
FixedThreadPool: submit 1.7, execution 2790
RoundRobin: submit 0.3, execution 16073
WorkStealing: submit 0.3, execution 3702

PARETO
ThreadPerTask: submit 24, execution 9217
FixedThreadPool: submit 1.9, execution 7718
RoundRobin: submit 0.2, execution 7964
WorkStealing: submit 0.2, execution 7255

Выводы:
Пулы работают стабильнее и обычно показывают себя лучше.
На равномерных задачах разница между ними небольшая.
На периодических задачах RoundRobin сильно проседает, потому что длинные задачи попадают в одну очередь.
WorkStealing и FixedThreadPool тут лучше, потому что свободные потоки забирают работу.
Замеры со сном довольно шумные, а с blackhole нагрузка более честная и понятная.
