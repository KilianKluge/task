<?php
declare(strict_types=1);

namespace Flowpack\Task\Domain\Repository;

use DateTime;
use Doctrine\DBAL\Types\Types;
use Doctrine\ORM\AbstractQuery;
use Doctrine\ORM\ORMException;
use Flowpack\Task\Domain\Model\TaskExecution;
use Neos\Flow\Annotations as Flow;
use Flowpack\Task\Domain\Task\Task;
use Flowpack\Task\Domain\Task\TaskStatus;
use Neos\Flow\Persistence\Doctrine\Repository;
use Neos\Flow\Persistence\Exception\IllegalObjectTypeException;
use Neos\Flow\Persistence\QueryInterface;
use Neos\Flow\Persistence\QueryResultInterface;

/**
 * @Flow\Scope("singleton")
 */
class TaskExecutionRepository extends Repository
{
    public function findPending(Task $task): QueryResultInterface
    {
        $query = $this->createQuery();
        $query->matching(
            $query->logicalAnd(
                $query->equals('taskIdentifier', $task->getIdentifier()),
                $query->logicalOr(
                    $query->equals('status', TaskStatus::PLANNED),
                    $query->equals('status', TaskStatus::RUNNING),
                )
            )
        );
        return $query->execute();
    }

    public function findByTask(Task $task): QueryResultInterface
    {
        $query = $this->createQuery();
        $query->matching(
            $query->equals('taskIdentifier', $task->getIdentifier()),
        );
        return $query->execute();
    }

    public function removePlannedTask(Task $task): void
    {
        $query = $this->createQuery();
        $query->matching(
            $query->logicalAnd(
                $query->equals('taskIdentifier', $task->getIdentifier()),
                $query->equals('status', TaskStatus::PLANNED)
            )
        );

        foreach ($query->execute() as $scheduledTask) {
            try {
                $this->remove($scheduledTask);
            } catch (ORMException|IllegalObjectTypeException $e) {
                throw new \RuntimeException('Failed to remove task from execution repository', 1645610863, $e);
            }
        }
    }

    public function removeByOptions($taskIdentifier, $stati, $before, bool $dry = true): array
    {
        $query = $this->createQuery();

        $constraints = [];
        if ($taskIdentifier) $constraints[] = $query->equals('taskIdentifier', $taskIdentifier);
        if ($stati) {
            if (strpos($stati, ",")!==false) {
                $statusConstraints = [];
                foreach (explode(',', $stati) as $status) {
                    if (substr($status, 0, 1)==="~") $constraints[] = $query->logicalNot($query->equals('status', substr($status, 1)));
                    else $statusConstraints[] = $query->equals('status', $status);
                }
                $constraints[] = $query->logicalOr($statusConstraints);
            } else {
                if (substr($stati, 0, 1)==="~") $constraints[] = $query->logicalNot($query->equals('status', substr($stati, 1)));
                else $constraints[] = $query->equals('status', $stati);
            }
        }
        if ($before) $constraints[] = $query->logicalOr(
            $query->lessThan('endTime', $before),
            $query->lessThan('scheduleTime', $before),
        );
        if ($constraints) {
            $query->matching(
                $query->logicalAnd(
                    $constraints
                )
            );
        }

        $targets = [];
        foreach ($query->execute() as $taskExecution) {
            try {
                if (!$dry) $this->remove($taskExecution);
                $targets[] = $taskExecution;
            } catch (ORMException|IllegalObjectTypeException $e) {
                throw new \RuntimeException('Failed to remove task from execution repository', 1645610863, $e);
            }
        }
        return $targets;
    }

    public function findLatestExecution(Task $task, int $limit = 5, int $offset = 0): QueryResultInterface
    {
        $query = $this->createQuery();

        $query->matching(
            $query->logicalAnd(
                $query->equals('taskIdentifier', $task->getIdentifier()),
                $query->logicalNot(
                    $query->equals('status', TaskStatus::PLANNED)
                )
            )
        )
            ->setOrderings(['scheduleTime' => QueryInterface::ORDER_DESCENDING]);

        if ($limit > 0) {
            $query->setLimit($limit);
        }

        if ($offset > 0) {
            $query->setOffset($offset);
        }

        return $query->execute();
    }

    public function findNextScheduled(DateTime $runTime, array $skippedExecutions = [], Task $task = null): ?TaskExecution
    {
        $queryBuilder = $this->createQueryBuilder('taskExecution');

        $queryBuilder
            ->where($queryBuilder->expr()->lte('taskExecution.scheduleTime', ':scheduleTime'))
            ->andWhere($queryBuilder->expr()->eq('taskExecution.status', ':status'))
            ->orderBy('taskExecution.scheduleTime', QueryInterface::ORDER_DESCENDING)
            ->setMaxResults(1)
            ->setParameter('scheduleTime', $runTime, Types::DATETIME_MUTABLE)
            ->setParameter('status', TaskStatus::PLANNED);

        if (!empty($skippedExecutions)) {
            $queryBuilder->andWhere(
                $queryBuilder->expr()->not($queryBuilder->expr()->in('taskExecution.Persistence_Object_Identifier', ':skippedExecutions'))
            )->setParameter('skippedExecutions', $skippedExecutions);
        }

        if ($task !== null) {
            $queryBuilder->andWhere(
                $queryBuilder->expr()->eq('taskExecution.taskIdentifier', ':taskIdentifier'))
                ->setParameter('taskIdentifier', $task->getIdentifier());
        }

        return $queryBuilder->getQuery()->getOneOrNullResult(AbstractQuery::HYDRATE_OBJECT);
    }


}
