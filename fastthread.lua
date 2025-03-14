--[[
FastThread - Высокопроизводительная библиотека для OpenComputers
Версия: 1.0.0
Автор: Claude

Библиотека предоставляет инструменты для максимально эффективной
работы с потоками, параллельным выполнением и оптимизацией.

Возможности:
- Управление корутинами с автоматической очисткой
- Создание пулов потоков с ограничением нагрузки
- Группировка потоков с возможностью совместного управления
- Защита от утечек памяти
- Быстрые примитивы синхронизации
- Обработка ошибок
]]

local component = require("component")
local thread = require("thread")
local event = require("event")
local computer = require("computer")

local fastthread = {}

-- Внутренние переменные
fastthread._internal = {
    all_threads = {},
    thread_groups = {},
    thread_counter = 0,
    monitor_thread = nil,
    crash_handlers = {},
    resource_limits = {
        max_memory_percent = 85,
        max_global_threads = 200,
    }
}

-- Инициализация библиотеки
function fastthread.init(options)
    options = options or {}
    
    -- Установка лимитов ресурсов
    if options.max_memory_percent then
        fastthread._internal.resource_limits.max_memory_percent = options.max_memory_percent
    end
    
    if options.max_global_threads then
        fastthread._internal.resource_limits.max_global_threads = options.max_global_threads
    end
    
    -- Запуск монитора ресурсов, если не запущен
    if not fastthread._internal.monitor_thread then
        fastthread._internal.monitor_thread = fastthread.create(function()
            while true do
                fastthread._checkResources()
                os.sleep(1)
            end
        end, "ResourceMonitor")
    end
    
    return fastthread
end

-- Проверяет ресурсы системы и принимает меры при необходимости
function fastthread._checkResources()
    local free_memory = computer.freeMemory()
    local total_memory = computer.totalMemory()
    local memory_used_percent = (1 - (free_memory / total_memory)) * 100
    
    -- Если памяти слишком мало, убиваем низкоприоритетные потоки
    if memory_used_percent > fastthread._internal.resource_limits.max_memory_percent then
        fastthread._killLowPriorityThreads()
    end
    
    -- Проверяем количество потоков
    if #fastthread._internal.all_threads > fastthread._internal.resource_limits.max_global_threads then
        fastthread._killOldThreads(fastthread._internal.resource_limits.max_global_threads * 0.8)
    end
end

-- Убивает низкоприоритетные потоки для освобождения памяти
function fastthread._killLowPriorityThreads()
    local threads = fastthread._internal.all_threads
    -- Сортируем потоки по приоритету (чем ниже число, тем ниже приоритет)
    table.sort(threads, function(a, b)
        return (a.priority or 0) < (b.priority or 0)
    end)
    
    -- Убиваем 30% наименее приоритетных потоков
    local kill_count = math.ceil(#threads * 0.3)
    for i = 1, kill_count do
        if threads[i] and threads[i].thread and threads[i].thread:status() ~= "dead" then
            threads[i].thread:kill()
            threads[i].status = "killed_by_resource_manager"
        end
    end
    
    fastthread._cleanDeadThreads()
end

-- Убивает старые потоки до указанного предела
function fastthread._killOldThreads(limit)
    local threads = fastthread._internal.all_threads
    -- Сортируем потоки по времени создания
    table.sort(threads, function(a, b)
        return (a.created_at or 0) < (b.created_at or 0)
    end)
    
    -- Убиваем старые потоки, чтобы сократить количество до предела
    while #threads > limit do
        if threads[1] and threads[1].thread and threads[1].thread:status() ~= "dead" then
            threads[1].thread:kill()
            threads[1].status = "killed_by_resource_manager"
        end
        table.remove(threads, 1)
    end
end

-- Очищает мертвые потоки из списка всех потоков
function fastthread._cleanDeadThreads()
    local alive_threads = {}
    for _, thread_info in ipairs(fastthread._internal.all_threads) do
        if thread_info.thread and thread_info.thread:status() ~= "dead" then
            table.insert(alive_threads, thread_info)
        end
    end
    fastthread._internal.all_threads = alive_threads
end

-- Создает новый поток и регистрирует его в системе
function fastthread.create(func, name, options)
    options = options or {}
    fastthread._internal.thread_counter = fastthread._internal.thread_counter + 1
    
    local thread_id = options.id or fastthread._internal.thread_counter
    local group = options.group
    local priority = options.priority or 5 -- По умолчанию средний приоритет (1-10)
    
    -- Создаем обертку для функции с обработкой ошибок
    local wrapped_func = function()
        local success, error = pcall(func)
        if not success and error then
            print(string.format("[FastThread] Error in thread %s: %s", 
                  name or thread_id, tostring(error)))
            
            -- Вызываем обработчики ошибок, если есть
            for _, handler in ipairs(fastthread._internal.crash_handlers) do
                pcall(handler, {
                    thread_id = thread_id,
                    name = name,
                    error = error,
                    group = group
                })
            end
        end
        
        -- Автоматически очищаем этот поток из списков
        fastthread._removeThreadById(thread_id)
    end
    
    -- Создаем поток
    local t = thread.create(wrapped_func)
    
    -- Регистрируем информацию о потоке
    local thread_info = {
        id = thread_id,
        name = name or ("Thread_" .. thread_id),
        thread = t,
        created_at = computer.uptime(),
        priority = priority,
        group = group,
        status = "running"
    }
    
    table.insert(fastthread._internal.all_threads, thread_info)
    
    -- Добавляем в группу, если указана
    if group then
        if not fastthread._internal.thread_groups[group] then
            fastthread._internal.thread_groups[group] = {}
        end
        table.insert(fastthread._internal.thread_groups[group], thread_info)
    end
    
    return t, thread_id
end

-- Создает группу потоков, которые выполняются параллельно
function fastthread.parallel(funcs, group_name, options)
    options = options or {}
    local threads = {}
    local results = {}
    
    for i, func in ipairs(funcs) do
        local thread_options = {
            group = group_name or ("ParallelGroup_" .. fastthread._internal.thread_counter),
            priority = options.priority,
            id = fastthread._internal.thread_counter + i
        }
        
        -- Создаем поток, который сохраняет результат
        local wrapped_func = function()
            results[i] = func()
        end
        
        local t, id = fastthread.create(wrapped_func, nil, thread_options)
        threads[i] = {thread = t, id = id}
    end
    
    -- Если нужно дождаться завершения
    if options.wait then
        fastthread.waitAll(threads)
    end
    
    return threads, results
end

-- Удаляет поток из списков по ID
function fastthread._removeThreadById(thread_id)
    -- Удаляем из основного списка
    for i, thread_info in ipairs(fastthread._internal.all_threads) do
        if thread_info.id == thread_id then
            table.remove(fastthread._internal.all_threads, i)
            break
        end
    end
    
    -- Удаляем из группы, если есть
    for group_name, group_threads in pairs(fastthread._internal.thread_groups) do
        for i, thread_info in ipairs(group_threads) do
            if thread_info.id == thread_id then
                table.remove(group_threads, i)
                break
            end
        end
    end
end

-- Ожидает завершения всех указанных потоков
function fastthread.waitAll(threads, timeout)
    local start_time = computer.uptime()
    
    while true do
        local all_done = true
        
        for _, t in ipairs(threads) do
            local thread_obj = t.thread or t -- Поддерживает как объекты потоков, так и thread_info
            if thread_obj:status() ~= "dead" then
                all_done = false
                break
            end
        end
        
        if all_done then
            return true
        end
        
        -- Проверяем таймаут
        if timeout and (computer.uptime() - start_time) > timeout then
            return false
        end
        
        os.sleep(0.05)
    end
end

-- Ожидает завершения любого из указанных потоков
function fastthread.waitAny(threads, timeout)
    local start_time = computer.uptime()
    
    while true do
        for i, t in ipairs(threads) do
            local thread_obj = t.thread or t
            if thread_obj:status() == "dead" then
                return i
            end
        end
        
        -- Проверяем таймаут
        if timeout and (computer.uptime() - start_time) > timeout then
            return nil
        end
        
        os.sleep(0.05)
    end
end

-- Убивает все потоки
function fastthread.killAll()
    for _, thread_info in ipairs(fastthread._internal.all_threads) do
        if thread_info.thread and thread_info.thread:status() ~= "dead" then
            thread_info.thread:kill()
        end
    end
    
    fastthread._internal.all_threads = {}
    fastthread._internal.thread_groups = {}
end

-- Убивает все потоки в указанной группе
function fastthread.killGroup(group_name)
    if not fastthread._internal.thread_groups[group_name] then
        return false
    end
    
    for _, thread_info in ipairs(fastthread._internal.thread_groups[group_name]) do
        if thread_info.thread and thread_info.thread:status() ~= "dead" then
            thread_info.thread:kill()
        end
    end
    
    fastthread._internal.thread_groups[group_name] = {}
    return true
end

-- Добавляет обработчик ошибок для потоков
function fastthread.onError(handler)
    table.insert(fastthread._internal.crash_handlers, handler)
end

-- Создает пул потоков с ограниченным количеством параллельных выполнений
function fastthread.createPool(size, options)
    options = options or {}
    local pool = {
        size = size,
        active = 0,
        queue = {},
        name = options.name or ("Pool_" .. fastthread._internal.thread_counter),
        threads = {},
        results = {},
        autoResize = options.autoResize or false,
        priority = options.priority or 5
    }
    
    -- Метод для добавления задачи в пул
    function pool:schedule(task, task_id)
        task_id = task_id or (#self.queue + 1)
        table.insert(self.queue, {
            task = task,
            id = task_id
        })
        self:_processQueue()
        return task_id
    end
    
    -- Метод для внутренней обработки очереди
    function pool:_processQueue()
        while #self.queue > 0 and self.active < self.size do
            local task_info = table.remove(self.queue, 1)
            local task_wrapper = function()
                self.active = self.active + 1
                
                local success, result = pcall(task_info.task)
                if success then
                    self.results[task_info.id] = result
                else
                    self.results[task_info.id] = {error = result}
                end
                
                self.active = self.active - 1
                self:_processQueue() -- Проверяем, нет ли новых задач
            end
            
            local thread_options = {
                group = "pool_" .. self.name,
                priority = self.priority
            }
            
            local t, id = fastthread.create(task_wrapper, "PoolTask_" .. task_info.id, thread_options)
            self.threads[task_info.id] = {thread = t, id = id}
        end
    end
    
    -- Метод для ожидания выполнения всех задач
    function pool:waitForAll(timeout)
        return fastthread.waitAll(self.threads, timeout)
    end
    
    -- Метод для ожидания конкретной задачи
    function pool:waitForTask(task_id, timeout)
        if not self.threads[task_id] then
            return nil, "Task not found"
        end
        
        local start_time = computer.uptime()
        
        while self.threads[task_id].thread:status() ~= "dead" do
            if timeout and (computer.uptime() - start_time) > timeout then
                return nil, "Timeout"
            end
            os.sleep(0.05)
        end
        
        return self.results[task_id]
    end
    
    -- Метод для изменения размера пула
    function pool:resize(new_size)
        self.size = new_size
        self:_processQueue() -- Запускаем новые задачи, если есть место
    end
    
    -- Метод для очистки пула
    function pool:clear()
        for _, thread_info in pairs(self.threads) do
            if thread_info.thread and thread_info.thread:status() ~= "dead" then
                thread_info.thread:kill()
            end
        end
        
        self.threads = {}
        self.queue = {}
        self.results = {}
        self.active = 0
    end
    
    return pool
end

-- Создает семафор для синхронизации потоков
function fastthread.createSemaphore(count)
    local sem = {
        count = count or 1,
        waiting = {}
    }
    
    -- Захват семафора
    function sem:acquire(timeout)
        local start_time = computer.uptime()
        
        while self.count <= 0 do
            local wait_id = tostring(math.random(1, 10000)) .. "_" .. computer.uptime()
            self.waiting[wait_id] = true
            
            -- Проверяем таймаут
            if timeout and (computer.uptime() - start_time) > timeout then
                self.waiting[wait_id] = nil
                return false
            end
            
            os.sleep(0.05)
            
            -- Если поток был разбужен
            if not self.waiting[wait_id] then
                if self.count > 0 then
                    self.count = self.count - 1
                    return true
                end
            end
        end
        
        -- Если семафор доступен
        self.count = self.count - 1
        return true
    end
    
    -- Освобождение семафора
    function sem:release()
        self.count = self.count + 1
        
        -- Будим один из ожидающих потоков
        for wait_id, _ in pairs(self.waiting) do
            self.waiting[wait_id] = nil
            break
        end
    end
    
    return sem
end

-- Создает мьютекс для взаимного исключения
function fastthread.createMutex()
    return fastthread.createSemaphore(1)
end

-- Запускает функцию с таймаутом
function fastthread.runWithTimeout(func, timeout, callback)
    local result = nil
    local done = false
    
    -- Запускаем основную функцию
    local main_thread = fastthread.create(function()
        result = func()
        done = true
    end, "TimeoutTask")
    
    -- Запускаем таймер
    local timer_thread = fastthread.create(function()
        os.sleep(timeout)
        if not done then
            main_thread:kill()
            done = true
            if callback then
                callback("timeout")
            end
        end
    end, "TimeoutTimer")
    
    -- Ждем завершения
    while not done do
        os.sleep(0.05)
    end
    
    -- Убиваем таймер, если основная функция завершилась
    if timer_thread:status() ~= "dead" then
        timer_thread:kill()
    end
    
    return result, done
end

-- Периодически выполняет функцию с заданным интервалом
function fastthread.interval(func, interval, options)
    options = options or {}
    local timer = {
        running = true,
        count = 0,
        thread = nil,
        last_run = computer.uptime()
    }
    
    -- Запускаем поток таймера
    timer.thread = fastthread.create(function()
        while timer.running do
            local current_time = computer.uptime()
            local elapsed = current_time - timer.last_run
            
            if elapsed >= interval then
                timer.count = timer.count + 1
                timer.last_run = current_time
                
                local success, result = pcall(func, timer)
                
                -- Остановка при ошибке, если задано
                if not success and options.stopOnError then
                    timer.running = false
                    print(string.format("[FastThread] Interval error: %s", tostring(result)))
                    break
                end
                
                -- Остановка по лимиту выполнений
                if options.maxRuns and timer.count >= options.maxRuns then
                    timer.running = false
                    break
                end
            end
            
            os.sleep(math.min(0.05, interval / 10))
        end
    end, "Interval_" .. fastthread._internal.thread_counter, {
        priority = options.priority or 3
    })
    
    -- Метод для остановки таймера
    function timer:stop()
        self.running = false
    end
    
    -- Метод для перезапуска таймера
    function timer:restart()
        self.running = true
        self.last_run = computer.uptime()
        if self.thread:status() == "dead" then
            -- Пересоздаем поток, если он уже завершен
            self.thread = fastthread.create(function()
                while timer.running do
                    local current_time = computer.uptime()
                    local elapsed = current_time - timer.last_run
                    
                    if elapsed >= interval then
                        timer.count = timer.count + 1
                        timer.last_run = current_time
                        
                        local success, result = pcall(func, timer)
                        
                        if not success and options.stopOnError then
                            timer.running = false
                            break
                        end
                        
                        if options.maxRuns and timer.count >= options.maxRuns then
                            timer.running = false
                            break
                        end
                    end
                    
                    os.sleep(math.min(0.05, interval / 10))
                end
            end, "Interval_" .. fastthread._internal.thread_counter, {
                priority = options.priority or 3
            })
        end
    end
    
    return timer
end

-- Возвращает статистику использования потоков
function fastthread.getStats()
    fastthread._cleanDeadThreads() -- Очищаем мертвые потоки перед получением статистики
    
    local stats = {
        total_threads = #fastthread._internal.all_threads,
        groups = {},
        memory_used_percent = (1 - (computer.freeMemory() / computer.totalMemory())) * 100,
        uptime = computer.uptime()
    }
    
    -- Считаем потоки по группам
    for group_name, group_threads in pairs(fastthread._internal.thread_groups) do
        local active_count = 0
        for _, thread_info in ipairs(group_threads) do
            if thread_info.thread and thread_info.thread:status() ~= "dead" then
                active_count = active_count + 1
            end
        end
        
        stats.groups[group_name] = {
            total = #group_threads,
            active = active_count
        }
    end
    
    return stats
end

-- Инициализируем библиотеку с настройками по умолчанию
fastthread.init()

return fastthread
